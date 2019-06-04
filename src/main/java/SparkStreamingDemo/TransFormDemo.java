package SparkStreamingDemo;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * 该transform操作（及其变体transformWith）允许在DStream上应用任意RDD到RDD功能。
 * 它可用于应用未在DStream API中公开的任何RDD操作。
 * 例如，将数据流中的每个批次与另一个数据集连接的功能不会直接在DStream API中公开。
 *      但是，您可以轻松地使用它transform来执行此操作。这使得非常强大的可能性。
 * 例如，可以通过将输入数据流与预先计算的垃圾邮件信息（也可以使用Spark生成）连接，
 *      然后根据它进行过滤，来进行实时数据清理。
 *
 * 请注意，在每个批处理间隔中都会调用提供的函数。这允许您进行时变RDD操作，即RDD操作，分区数，广播变量等可以在批次之间进行更改。
 */
public class TransFormDemo {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf=new SparkConf().setAppName("transform操作示例").setMaster("local[4]");
        JavaStreamingContext jssc=new JavaStreamingContext(conf, Durations.seconds(3));
        JavaReceiverInputDStream<String> lines=jssc.socketTextStream("192.168.179.128",9999);
        JavaDStream<String> word = lines.transform(rdd -> rdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator()));
        JavaPairDStream<String,Integer> pairs=word.mapToPair(x->new Tuple2<>(x,1));
        JavaPairDStream<String,Integer> wordcounts=pairs.reduceByKey((i1,i2)->i1+i2);
        wordcounts.print();

        jssc.start();              // 开始计算
        jssc.awaitTermination();   // 等待计算结束
    }
}
