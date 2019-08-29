package com.wangwei.kafka.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

/**
 *
 */
public class CustomerOffsetConsumer {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.113:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        // 关闭自动提交
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList("thread-test"), new ConsumerRebalanceListener() {
            /**
             * 如果第一次启动消费者，则无回收分区，并且该消费者分配所有的分区
             * 如果此时再启动一个消费者，则回收所有分区，并按照负载策略重新分配分区(range[默认的])
             */
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                System.out.println("========回收分区========");
                for (TopicPartition partition : collection) {
                    System.out.println("partition = " + partition);
                }
            }

            // 定位新分配分区的offset
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                System.out.println("========重新分配的分区=========");
                for (TopicPartition partition : collection) {
                    System.out.println("partition = " + partition);
                }
                for (TopicPartition partition : collection) {
                    // 得到当前分区下的offset,考虑多个topic
                    long offset = getPartitionOffset(partition);
                    consumer.seek(partition, offset);
                }
            }
        });
        // 拉取数据
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord record : records) {
                System.out.println("topic=" + record.topic() + ",offset=" + record.offset() + ",partition=" + record.partition() + ",value=" + record.value());
                TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                commitOffset(topicPartition, record.offset() + 1);
            }
        }
    }

    private static void commitOffset(TopicPartition topicPartition, long offset) {
    }

    private static long getPartitionOffset(TopicPartition partition) {
        // 应该从外部保存的地方读取
        return 0;
    }
}
