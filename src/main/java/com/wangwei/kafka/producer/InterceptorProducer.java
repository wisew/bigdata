package com.wangwei.kafka.producer;

import com.wangwei.kafka.interceptor.CounterInterceptor;
import com.wangwei.kafka.interceptor.TimeInterceptor;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 配置生产者
 * 创建生产者对象
 * 调用send方法
 */
public class InterceptorProducer {
    public static void main(String[] args) {
        List<String> interceptors = new ArrayList<>();
        interceptors.add(TimeInterceptor.class.getName());
        interceptors.add(CounterInterceptor.class.getName());
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.113:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 32 * 1024 * 1024);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int i = 0; i < 1000; i++) {
            producer.send(new ProducerRecord<>("test", i + "", "message--" + i));
        }
        producer.close();
    }
}
