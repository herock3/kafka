package com.shu.zyx.kafkaStorm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.topology.TopologyBuilder;

import java.util.UUID;


/**
 * Created by zyx on 2018/4/27.
 */
public class MainTopology {
    private static final String BOLT_ID = SplitBolt.class.getName();
    private static final String SPOUT_ID = KafkaSpout.class.getName();

    public static void main(String[] args){
        TopologyBuilder builder = new TopologyBuilder();
        //表示kafka使用的zookeeper的地址
        String brokerZkStr = "cdh1:2181,cdh2:2181,cdh3:2181";
        ZkHosts zkHosts = new ZkHosts(brokerZkStr);
        //表示的是kafak中存储数据的主题名称
        String topic = "tradingSystem";
        //指定zookeeper中的一个根目录，里面存储kafkaspout读取数据的位置等信息
        String zkRoot = "/kafkaspout";
        String id = UUID.randomUUID().toString();
        SpoutConfig spoutconf  = new SpoutConfig(zkHosts, topic, zkRoot, id);
        builder.setSpout(SPOUT_ID , new KafkaSpout(spoutconf),1);
        builder.setBolt(BOLT_ID,new SplitBolt(),1).shuffleGrouping(SPOUT_ID);

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology(MainTopology.class.getSimpleName(), new Config(),builder.createTopology() );

    }
}
