package br.com.alura.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try(
            var orderDispatcher = new KafkaDispatcher<Order>();
            var emailDispacher = new KafkaDispatcher<String>();
        ) {
            for (int i = 0; i < 10; i++) {

                var userId = UUID.randomUUID().toString();
                var orderId = UUID.randomUUID().toString();
                var amount = new BigDecimal(Math.random() * 5000 + 1);
                var email = Math.random() + "@email.com";

                var order = new Order(orderId, amount, email);

                var value = userId + ",67523,1234";
                orderDispatcher.send("ECOMMERCE_NEW_ORDER", email, order);

//                var email = new Email("subject@mail.com", "Thank you for your order! We are processing your order");
                var emailCode = "Thank you for your order! We are processing your order";
                emailDispacher.send("ECOMMERCE_SEND_EMAIL", email, emailCode);
            }
        }
    }
}
