package com.shu.zyx.kafka.single;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
/**
 * Created by zyx on 2018/4/25.
 */
public class KafkaProducerSimple {
    public static void main(String[] args) throws IOException {
        /**
         * 1 参数设置
         */
        Properties props = new Properties();
        //kafka集群地址
        props.put("bootstrap.servers", "cdh1:9092,cdh2:9092,cdh3:9092");
//        ack机制，acks=0:设置为0表示producer不需要等待任何确认收到的信息。acks=1： 这意味着至少要等待leader已经成功将数据写入本地log， acks=all： 这意味着leader需要等待所有备份都成功写入日志
        props.put("acks", "1");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        String topic = "orderMq";
        /**
         * 2 初始化生产者
         */
        Producer<String, String> producer = new KafkaProducer<String, String>(props);
        File file = new File("E:/test.txt");
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String tempString = null;
            int line = 1;
            while ((tempString = reader.readLine()) != null) {
                /**
                 * 发送消息，格式：topic+key+value,其中key/value的类型要与key.serializer和value.serializer对应
                 */
                producer.send(new ProducerRecord<String, String>(topic,line+"---" + tempString));
                System.out.println("Success send [" + line + "] message ..");
                line++;
            }
            reader.close();
            System.out.println("Total send [" + line + "] messages ..");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {}
            }
        }
        producer.close();

    }
}

