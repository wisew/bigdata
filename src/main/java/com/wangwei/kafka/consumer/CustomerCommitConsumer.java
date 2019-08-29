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
 * 如果不提交，offset无法更新，重新启动消费者时会重复消费
 * 提交分为sync和async
 */
public class CustomerCommitConsumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.1.113:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test1");
        // 关闭自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // 设置从哪里消费
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
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
            // 设置异步提交,是在消费完成之后提交，保证at least once
            System.out.println(num);
            consumer.commitSync();
        }
    }
}
