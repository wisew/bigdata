package com.wangwei.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 定义回掉函数,方法参数
 * RecordMetadata
 * exception:如果为空，则代表消息发送成功
 * 如果不为空，则发送失败，kafka会自动重发，不需要我们设置
 */
public class CustomerProducerWithCallback {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.113:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 32 * 1024 * 1024);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i = 0; i < 100; i++) {
            producer.send(new ProducerRecord<String, String>("test", i + "", "message--" + i), (recordMetadata, e) -> {
                if (e == null){
                    System.out.println("发送成功");
                } else {
                    e.printStackTrace();
                }
            });
        }
        producer.close();
    }
}
