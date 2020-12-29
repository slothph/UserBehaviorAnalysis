package com.slothph.hotitems_analysis;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Properties;

/**
 * @author hao.peng01@hand-china.com 2020/12/28 16:36
 */
public class KafkaProducerUtil {
    public static void main(String[] args) throws Exception {
        writeToKafka("hotitems");

    }


    public static void writeToKafka(String topic) throws Exception {

        Properties properties = new Properties();
//        properties.setProperty("zookeeper.connect", "172.23.16.112:2181");
        properties.setProperty("bootstrap.servers", "172.23.16.112:6667");
        properties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //定义一个Kafka Producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //用缓冲方式读取文本
        BufferedReader bufferedReader = new BufferedReader(new FileReader("D:\\WorkSpace\\GitDepository\\Code\\UserBehaviorAnalysis\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv"));
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, line);
            kafkaProducer.send(producerRecord);
        }
        kafkaProducer.close();

    }
}
