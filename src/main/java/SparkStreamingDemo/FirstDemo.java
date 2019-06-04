package SparkStreamingDemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

public class FirstDemo {
    public static void main(String[] args) throws InterruptedException {
        //创建一个本地StreamingContext，其中有两个工作线程，批处理间隔为1秒
        SparkConf conf=new SparkConf().setMaster("local[2]").setAppName("第一个SparkStreaming程序");
        JavaStreamingContext jssc=new JavaStreamingContext(conf, Durations.seconds(1));

        //也可以用jsc创建JavaStreamingContext
    //    JavaSparkContext jsc=new JavaSparkContext(conf);
    //    JavaStreamingContext jscc=new JavaStreamingContext(jsc,Durations.seconds(1));


        //创建一个连接到hostname:port的DStream，比如localhost:9999
        JavaReceiverInputDStream<String> lines=jssc.socketTextStream("192.168.179.128",9999);
        //此linesDStream表示将从数据服务器接收的数据流。此流中的每条记录都是一行文本。然后，我们想要将空格分割为单词。
        JavaDStream<String> words=lines.flatMap(x-> Arrays.asList(x.split(",")).iterator());
       words=lines.flatMap(x->Arrays.asList(x.split(" ")).iterator());
        //在每一批中计算每一个单词
        JavaPairDStream<String,Integer> pairs=words.mapToPair(s->new Tuple2<>(s,1));
        JavaPairDStream<String,Integer> wordcounts=pairs.reduceByKey((i1,i2)->i1+i2);
        //将此DStream中生成的每个RDD的前十个元素打印到控制台
        wordcounts.print();

        jssc.start();              // 开始计算
        jssc.awaitTermination();   // 等待计算结束
    }
}
