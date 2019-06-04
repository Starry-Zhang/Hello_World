package SparkStreamingDemo;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

/**
 * 流连接
 * Streams可以很容易地与其他流连接。
 */
public class JoinStreamDemo {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setAppName("流之间的连接").setMaster("local[4]");
        JavaStreamingContext jssc=new JavaStreamingContext(conf, Durations.seconds(3));
        JavaDStream<String> line1=jssc.socketTextStream("192.168.179.128",9999);
        JavaDStream<String> line2=jssc.socketTextStream("192.168.179.128",9999);

        JavaPairDStream<String ,String> pair1=line1.mapToPair(x->new Tuple2(x,"接收"));
        JavaPairDStream<String ,String> pair2=line2.mapToPair(x->new Tuple2<>("x","成功"));

        //连接
        JavaPairDStream<String,Tuple2<String,String>> joinString=pair1.join(pair2);
        //报错，好像JavaDStream之间不能连接 line1.join(line2);

        JavaPairDStream<String, String> windowedStream1 = pair1.window(Durations.seconds(20));
        JavaPairDStream<String, String> windowedStream2 = pair2.window(Durations.minutes(1));
        JavaPairDStream<String, Tuple2<String, String>> joinedStream = windowedStream1.join(windowedStream2);

        joinString.print();



        jssc.start();
        jssc.awaitTermination();
    }
}
