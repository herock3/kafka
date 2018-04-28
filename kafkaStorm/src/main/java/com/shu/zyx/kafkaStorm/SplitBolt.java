package com.shu.zyx.kafkaStorm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by Administrator on 2018/4/27.
 */
public class SplitBolt extends BaseRichBolt {
    OutputCollector collector;
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector=this.collector;
    }

    public void execute(Tuple input) {
        byte[] bytes = input.getBinaryByField("bytes");
        String value = new String(bytes).replaceAll("[\n\r]", "");
        System.out.println(value);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
