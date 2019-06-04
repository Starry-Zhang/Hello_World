package StructStreaming;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.Arrays;

/**
 * 为什么报错
 */

public class FirstDemo {
    public static void main(String[] args) throws StreamingQueryException {
      //  System.setProperty("hadoop.home.dir", "C:\\开发软件\\Hadoop\\hadoop-common-2.2.0-bin-master");

        SparkSession spark=SparkSession
                .builder()
                .appName("侦听TCP套接字")
                .getOrCreate();
        //创建一个流式DataFrame，它表示从侦听localhost：9999的服务器接收的文本数据，并转换DataFrame以计算字数。
        Dataset<Row> lines=spark
                .readStream()
                .format("socket")
                .option("host","localhost")//"192.168.179.128"
                .option("port",9999)
                .load();
        // Split the lines into words
        //把这些行分成单词
        Dataset<String> words=lines
                .as(Encoders.STRING())
                .flatMap((FlatMapFunction<String,String>)x-> Arrays.asList(x.split(" ")).iterator(),Encoders.STRING());
        //生成运行数字
        Dataset<Row> wordCounts=words.groupBy("value").count();

        //开始运行查询，并将运行计数打印到控制台
        StreamingQuery query = wordCounts.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();//awaitTermination()以防止在查询处于活动状态时退出该进程。
    }
}
