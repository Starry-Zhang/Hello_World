package SparkStreamingDemo;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class CheckPointDemo {
    public static void main(String[] args) {
        //创建一个工厂对象，该对象可以创建和设置一个新的JavaStreamingContext
        SparkConf conf=new SparkConf().setMaster("local[2]").setAppName("检查点示例");
        JavaStreamingContext jssc=new JavaStreamingContext(conf, Durations.seconds(1));
        JavaDStream<String> lines2= jssc.textFileStream("C:\\临时文件\\testSparkStreaming");
        //创建检查点
        jssc.checkpoint("C:\\临时文件\\testSparkStreaming");

        //从检查点数据获取JavaStreamingContext或创建一个新的？
        //？？

        //看同一个包下的JavaRecoverableNetworkWordCount
    }
}
