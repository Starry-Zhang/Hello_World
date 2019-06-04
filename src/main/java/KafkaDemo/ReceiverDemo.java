package KafkaDemo;

import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;

import java.util.HashMap;
import java.util.Map;

public class ReceiverDemo {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("kafka和SparkStreaming的Receiver连接方式")
                .setMaster("local[4]")
                .set("spark.streaming.receiver.writeAheadLog.enable","true");
        JavaStreamingContext jsc=new JavaStreamingContext(conf, Durations.seconds(3));
        jsc.checkpoint("C:\\IDEA_workSpace\\againDemo\\checkpointDir");
        Map<String,Integer> topicConsumerConcurrency=new HashMap<String,Integer>();
        //topic名    receiver 数量
        topicConsumerConcurrency.put("test_create_topic",1);
       // JavaPairInputDStream<String,String> lines=
              //  KafkaUtils.createDirectStream()
//                        jsc,
//                        "192.168.179.128:9092",
//                        "firstConsumerGroupID",
//                        topicConsumerConcurrency,
//                        StorageLevel.MEMORY_AND_DISK_SER()
//                );
    }
}
