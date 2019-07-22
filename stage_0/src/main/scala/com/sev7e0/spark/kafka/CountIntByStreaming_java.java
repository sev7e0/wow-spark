package com.sev7e0.spark.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class CountIntByStreaming_java {

    public static final String brokerList = "localhost:9092";
    public static final String topic = "randomCount";
    //新的group，相较于ConsumerQuickStart group-1分组，现在kafka是发布订阅模型
    public static final String groupId = "group";
    public static final String path = "temp/checkpoint/CountIntBySS";
    public static final String master = "local";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName(CountIntByStreaming_java.class.getName())
                .setMaster(master);

        JavaStreamingContext context = new JavaStreamingContext(conf, Duration.apply(2000));

        context.checkpoint(path);

        Map<String, Object> kafkaParams = initProperties();
        JavaInputDStream<ConsumerRecord<String, String>> dStream = KafkaUtils.createDirectStream(context,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(Arrays.asList(topic), kafkaParams));

        dStream.mapToPair(record -> new Tuple2<>(record.key(), Integer.valueOf(record.value())))
                .reduce(CountIntByStreaming_java::call)
                .print();

        context.start();
        try {
            context.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    /**
     * 初始化配置
     *
     * @return
     */
    private static Map<String, Object> initProperties() {
        Map<String, Object> kafkaParams = new HashMap<>();

        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);

        //关闭kafka默认的自动提交offset，容易导致重复处理的问题
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return kafkaParams;
    }

    private static Tuple2<String, Integer> call(Tuple2<String, Integer> v1, Tuple2<String, Integer> v2) {
        return new Tuple2<>(null, v1._2 + v2._2);
    }
}
