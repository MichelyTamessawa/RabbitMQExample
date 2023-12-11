package com.example.rabbitmq;

import android.os.Bundle;
import android.util.Log;

import androidx.appcompat.app.AppCompatActivity;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

public class MainActivity extends AppCompatActivity {
    public static final String QUEUE_NAME = "hello";
    private ConnectionFactory factory;
    private Thread publishThread;
    private Thread subscribeThread;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
        factory = new ConnectionFactory();
        factory.setHost("10.0.2.2");
        factory.setUsername("guest");
        factory.setPassword("guest");
        subscribe();
    }

    private void send() {
        publishThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Connection connection = factory.newConnection();
                    Channel channel = connection.createChannel();
                    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
                    String mensagem = "Olá Mundo!";
                    channel.basicPublish("", QUEUE_NAME, null, mensagem.getBytes(StandardCharsets.UTF_8));
                    Log.i("RabbitMQ", "Sending message");
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
        publishThread.start();
    }

    void subscribe() {
        subscribeThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Connection connection = factory.newConnection();
                    Channel channel = connection.createChannel();
                    channel.queueDeclare(QUEUE_NAME, false, false, false, null);
                    Log.i("RabbitMQ", "Waiting for messages");
                    DeliverCallback deliverCallback = ((consumerTag, message) -> {
                        String messageText = new String(message.getBody(), StandardCharsets.UTF_8);
                        Log.i("RabbitMQ", "Received: " + messageText);
                        channel.basicAck(message.getEnvelope().getDeliveryTag(), false);
                    });
                    channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> {
                    });
                } catch (Exception e1) {
                    Log.e("RabbitMQ", "Connection broken: " + e1.getClass().getName());
                    try {
                        Thread.sleep(5000); //sleep and then try again
                    } catch (InterruptedException e) {
                    }
                }
            }
        });
        subscribeThread.start();
    }
}