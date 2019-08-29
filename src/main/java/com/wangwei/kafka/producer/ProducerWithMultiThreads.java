package com.wangwei.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ProducerWithMultiThreads {
    private static CountDownLatch latch = new CountDownLatch(3);
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.1.113:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, 32 * 1024 * 1024);
        properties.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        ExecutorService service = Executors.newCachedThreadPool();
        service.submit(new ProducerThread(producer));
        service.submit(new ProducerThread(producer));
        service.submit(new ProducerThread(producer));
        service.shutdown();
        latch.await();
        producer.close();

    }
    private static class ProducerThread implements Runnable{
        private KafkaProducer<String,String> producer;
        public ProducerThread(KafkaProducer<String,String> producer){
            this.producer = producer;
        }
        @Override
        public void run() {
            for (int i = 0; i < 1000; i++) {
                producer.send(new ProducerRecord<String,String>("thread-test", i + "", "message--" + i));
            }
            latch.countDown();
        }
    }
}
