package com.xiaoding.wordcount;

import com.xiaoding.wordcount.bolt.ReportBolt;
import com.xiaoding.wordcount.bolt.SplitSentenceBolt;
import com.xiaoding.wordcount.bolt.WordCountBolt;
import com.xiaoding.wordcount.spout.SentenceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by xiaoding on 11/2 0002.
 */
public class WordCountTopology {
    private static final Logger log = LoggerFactory.getLogger(WordCountTopology.class);

    //各个组件名字的唯一标识
    private final static String SENTENCE_SPOUT_ID = "sentence-spout";
    private final static String SPLIT_SENTENCE_BOLT_ID = "split-bolt";
    private final static String WORD_COUNT_BOLT_ID = "count-bolt";
    private final static String REPORT_BOLT_ID = "report-bolt";

    //拓扑名称
    private final static String TOPOLOGY_NAME = "word-count-topology";

    public static void main(String[] args) throws Exception {

        log.info(".........begining.......");
        //各个组件的实例
        SentenceSpout sentenceSpout = new SentenceSpout();
        SplitSentenceBolt splitSentenceBolt = new SplitSentenceBolt();
        WordCountBolt wordCountBolt = new WordCountBolt();
        ReportBolt reportBolt = new ReportBolt();

        //构建一个拓扑Builder
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        //配置第一个组件sentenceSpout
        topologyBuilder.setSpout(SENTENCE_SPOUT_ID, sentenceSpout, 2);

        //配置第二个组件splitSentenceBolt,上游为sentenceSpout,tuple分组方式为随机分组shuffleGrouping
        topologyBuilder.setBolt(SPLIT_SENTENCE_BOLT_ID, splitSentenceBolt).shuffleGrouping(SENTENCE_SPOUT_ID);

        //配置第三个组件wordCountBolt,上游为splitSentenceBolt,tuple分组方式为fieldsGrouping,同一个单词将进入同一个task中(bolt实例)
        topologyBuilder.setBolt(WORD_COUNT_BOLT_ID, wordCountBolt).fieldsGrouping(SPLIT_SENTENCE_BOLT_ID, new Fields("word"));

        //配置最后一个组件reportBolt,上游为wordCountBolt,tuple分组方式为globalGrouping,即所有的tuple都进入这一个task中
        topologyBuilder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(WORD_COUNT_BOLT_ID);

        Config config = new Config();

        //建立本地集群,利用LocalCluster,storm在程序启动时会在本地自动建立一个集群,不需要用户自己再搭建,方便本地开发和debug
        LocalCluster cluster = new LocalCluster();

        //创建拓扑实例,并提交到本地集群进行运行
        if (args != null && args.length > 0) {
            config.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(args[0], config, topologyBuilder.createTopology());
        }
        else {
            config.setMaxTaskParallelism(3);

            LocalCluster localcluster = new LocalCluster();

            localcluster.submitTopology(TOPOLOGY_NAME, config, topologyBuilder.createTopology());

            Thread.sleep(10000);

            cluster.shutdown();
        }

    }
}
