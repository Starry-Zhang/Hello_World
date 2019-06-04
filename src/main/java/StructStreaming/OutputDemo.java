package StructStreaming;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

import java.util.Arrays;

public class OutputDemo {
    public static void main(String[] args) {
        SparkSession spark=SparkSession
                .builder()
                .appName("侦听TCP套接字")
                .getOrCreate();
        Dataset<Row> lines=spark
                .readStream()
                .format("socket")
                .option("host","localhost")//"192.168.179.128"
                .option("port",9999)
                .load();
        Dataset<String> words=lines
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String,String>) x-> Arrays.asList(x.split(" ")).iterator(),Encoders.STRING());

        Dataset<Row> wordCounts=words.groupBy("value").count();

        //输出接收器

        //文件接收器-将输出存储到目录
        StreamingQuery query=wordCounts.writeStream()
                .format("parquet")        // can be "orc", "json", "csv", etc.
                .option("path", "path/to/destination/dir")
                .start();
        //Kafka sink - 将输出存储到Kafka中的一个或多个主题。
        StreamingQuery query2=wordCounts.writeStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
                .option("topic", "updates")
                .start();
        //调用start()实际开始执行查询。这将返回一个StreamingQuery对象，该对象是持续运行的执行的句柄。您可以使用此对象来管理查询，
    }
}
