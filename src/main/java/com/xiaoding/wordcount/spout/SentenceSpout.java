package com.xiaoding.wordcount.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by xiaoding on 11/2 0002.
 */
public class SentenceSpout extends BaseRichSpout {
    private SpoutOutputCollector spoutOutputCollector;

    //为了简单,定义一个静态数据模拟不断的数据流产生
    private static final String[] sentences = {
            "The logic for a realtime application is packaged into a Storm topology",
            "A Storm topology is analogous to a MapReduce job",
            "One key difference is that a MapReduce job eventually finishes whereas a topology runs forever",
            " A topology is a graph of spouts and bolts that are connected with stream groupings"
    };

    private int index = 0;

    //初始化操作
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    //核心逻辑
    public void nextTuple() {
        spoutOutputCollector.emit(new Values(sentences[index]));
        ++index;
        if (index >= sentences.length) {
            index = 0;
        }
    }

    //向下游输出
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentences"));
    }
}
