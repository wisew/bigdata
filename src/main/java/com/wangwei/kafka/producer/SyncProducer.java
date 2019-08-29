package com.wangwei.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * producer.send返回Future对象
 * 消息发送到RecordAccumulator
 * RecordAccumulator有batch机制，现在发送出去之后便阻塞，等到acks
 * batch达不到，则LINGER_MS(sender去RecordAccumulator取值的等待时间)起作用
 */
public class SyncProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.113:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 32 * 1024 * 1024);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 100);
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 100; i++) {
            RecordMetadata metadata = producer.send(new ProducerRecord<String, String>("test", i + "", "message--" + i)).get();
            System.out.println("offset="+metadata.offset());
        }
        producer.close();
    }
}
