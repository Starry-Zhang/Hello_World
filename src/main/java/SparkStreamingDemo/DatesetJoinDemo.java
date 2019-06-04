package SparkStreamingDemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

public class DatesetJoinDemo {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("数据集之间的连接").setMaster("local[4]");
        JavaStreamingContext jssc=new JavaStreamingContext(conf, Durations.seconds(3));
        JavaDStream<String> line1=jssc.textFileStream("C:\\临时文件\\testSparkStreaming");


        JavaPairDStream<String ,String> pair1=line1.mapToPair(x->new Tuple2(x,"接收"));

        //数据基金
        JavaSparkContext jsc=new JavaSparkContext(conf);
        JavaRDD<String> lines=jsc.textFile("hello.txt");



        //连接
        JavaPairRDD<String, String> dataset = lines.mapToPair(s->new Tuple2<>(s,"成功"));
        JavaPairDStream<String, String> windowedStream = pair1.window(Durations.seconds(20));
       // windowedStream.transform(rdd->rdd.reduceByKey(x->x+"xx"));

    }
}
