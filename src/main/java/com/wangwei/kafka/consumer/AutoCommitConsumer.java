package com.wangwei.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * 创建consumer
 * 拉取数据poll
 * 使用kafka的自动提交,可以配置对应多长时间自动提交一次
 */
public class AutoCommitConsumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.1.113:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        // 配置自动提交offset的时间间隔
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList("thread-test"));
        // 拉取数据
        int num = 0;
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            num += records.count();
            for (ConsumerRecord record:records){
                System.out.println("topic="+record.topic()+",offset="+record.offset()+",value="+record.value());
            }
            System.out.println(num);
        }
    }
}
