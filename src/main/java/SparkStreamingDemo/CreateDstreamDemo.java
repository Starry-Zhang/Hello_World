package SparkStreamingDemo;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class CreateDstreamDemo {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf=new SparkConf().setAppName("多种来源创建Dstream").setMaster("local[4]");
        JavaStreamingContext jssc=new JavaStreamingContext(conf, Durations.seconds(3));

        //创建一个连接到hostname:port的DStream，比如localhost:9999
       // JavaReceiverInputDStream<String> lines=jssc.socketTextStream("localhost",9999);

        //文件流
        JavaDStream<String> lines2= jssc.textFileStream("C:\\临时文件\\testSparkStreaming");

        //注意：监听程序只监听”C:\临时文件\testSparkStreaming”目录下在程序启动后新增的文件，
        //          不会去处理历史上已经存在的文件。
        JavaPairDStream<String,Integer> pairs=lines2.mapToPair(s->new Tuple2<>(s,1));
        JavaPairDStream<String,Integer> wordcounts=pairs.reduceByKey((i1,i2)->i1+i2);
        wordcounts.print();
        jssc.start();
        jssc.awaitTermination();


        //基于自定义接收器的流
        //RDD作为流的的队列streamingContext.queueStream(queueOfRDDs)。
    }
}
