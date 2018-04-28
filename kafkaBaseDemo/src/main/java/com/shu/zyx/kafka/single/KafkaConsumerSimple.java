package com.shu.zyx.kafka.single;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by zyx on 2018/4/26.
 */
public class KafkaConsumerSimple {
    public static void main(String[] args){
        Properties props = new Properties();
        props.put("bootstrap.servers", "cdh1:9092");
        props.put("group.id", "zyx");//不同ID 可以同时订阅消息
        props.put("enable.auto.commit", "true");//如果value合法，则自动提交偏移量
        props.put("auto.commit.interval.ms", "1000");//设置多久一次更新被消费消息的偏移量
        props.put("session.timeout.ms", "30000"); //设置会话响应的时间，超过这个时间kafka可以选择放弃消费或者消费下一条消息
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList("tradingSystem"));//订阅TOPIC
        try {
            while(true) {//轮询
                //ConsumerRecords包含了consumer所消费的分区以及每个分区的Records
                ConsumerRecords<String, String> records =consumer.poll(Long.MAX_VALUE);//超时等待时间
                for (TopicPartition partition : records.partitions()) {
                    //records.records(partition)获取每个分区的所有Records
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);

                    for (ConsumerRecord<String, String> record : partitionRecords) {

                        System.out.println(record.offset() + ": " + record.value());
                    }
                    consumer.commitSync();//同步
                }
            }
        } finally {
            try {
                Thread.sleep(1000);  //consumer隔一定时间提交offset至zookeeper,若提交前comsumer关闭，则会产生重复消费
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            consumer.close();
        }

    }

}
