package com.shu.zyx.kafka.muti;

/**
 * Created by Administrator on 2018/4/28.
 */
public class ConsumerMain {
    public static void main(String[] args) {
        String brokerList = "cdh1:9092";
        String groupId = "zyx";
        String topic = "orderMq";
        int consumerNum = 5;

        ConsumerGroup consumerGroup = new ConsumerGroup(consumerNum, groupId, topic, brokerList);
        consumerGroup.execute();
    }
}
