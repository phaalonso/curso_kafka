package br.com.alura.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

class KafkaService<T> implements Closeable {
    private final KafkaConsumer<String, T> consumer;
    private final ConsumerFuncion parse;

    KafkaService(String groupId, String topic, ConsumerFuncion parse, Class<T> type, Map<String, String> properties) {
        this(parse, groupId, type, properties);

        consumer.subscribe(Collections.singleton(topic));
    }

    public KafkaService(String groupId, Pattern compile, ConsumerFuncion parse, Class<T> type, Map<String, String> properties) {
        this(parse, groupId, type, properties);

        consumer.subscribe(compile);
    }

    private KafkaService(ConsumerFuncion<T> parse, String groupId, Class<T> type, Map<String, String> props) {
        this.parse = parse;
        this.consumer = new KafkaConsumer<>(properties(type, groupId, props));
    }

    void run() {
        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()) {
                System.out.println("Encontrei " + records.count() + " registros");
                for (var record : records) {
                    try {
                        parse.consume(record);
                    } catch (Exception e) {
                        // only catcher Exception because no matter which Exception
                        // i want to recover and parse the next one
                        // so far, just logging the exception for this message
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    private Properties properties(Class<T> type,String groupId, Map<String, String> overrideProps) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, GsonDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(GsonDeserializer.TYPE_CONFIG, type.getName());

        properties.putAll(overrideProps);
        return properties;
    }

    @Override
    public void close() {
        consumer.close();
    }
}
