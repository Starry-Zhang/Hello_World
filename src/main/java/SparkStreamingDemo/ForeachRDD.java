package SparkStreamingDemo;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.spark_project.jetty.client.ConnectionPool;

import java.sql.Connection;
import java.util.Arrays;

/**
 * dstream.foreachRDD是一个功能强大的原语，允许将数据发送到外部系统。
 *
 * 通常将数据写入外部系统需要创建连接对象（例如，与远程服务器的TCP连接）并使用它将数据发送到远程系统。
 */
public class ForeachRDD {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf=new SparkConf().setAppName("使用foreachRDD的设计模式").setMaster("local[4]");
        JavaStreamingContext jssc=new JavaStreamingContext(conf, Durations.seconds(5));
        JavaDStream<String> dstream=jssc.socketTextStream("192.168.179.128",9999);
        dstream.foreachRDD(rdd -> {
            rdd.foreachPartition(partitionOfRecords -> {
               if(partitionOfRecords.hasNext()){
                   System.out.println(partitionOfRecords.next());
               }
            });
        });

        jssc.start();
        jssc.awaitTermination();

    }
}
