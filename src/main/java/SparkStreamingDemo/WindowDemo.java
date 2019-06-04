package SparkStreamingDemo;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class WindowDemo {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf=new SparkConf().setAppName("Spark Streaming的窗口欧操作").setMaster("local[4]");
        JavaStreamingContext jssc=new JavaStreamingContext(conf, Durations.seconds(5));
        JavaDStream<String> line2=jssc.textFileStream("C:\\临时文件\\testSparkStreaming");
        JavaDStream<String> word=line2.flatMap(x-> Arrays.asList(x.split(" ")).iterator());
        JavaPairDStream<String,Integer> pair=word.mapToPair(x-> new Tuple2(x,1));
        //每10秒，处理最后30秒内的数据
        JavaPairDStream<String, Integer> windowedWordCounts = pair.reduceByKeyAndWindow((i1, i2) -> i1 + i2, Durations.seconds(30), Durations.seconds(10));


        windowedWordCounts.print();
        jssc.start();
        jssc.awaitTermination();
    }
}
