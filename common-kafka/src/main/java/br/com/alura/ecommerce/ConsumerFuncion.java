package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.sql.SQLException;
import java.util.concurrent.ExecutionException;

public interface ConsumerFuncion<T> {
    void consume(ConsumerRecord<String, T> parse) throws Exception;
}
