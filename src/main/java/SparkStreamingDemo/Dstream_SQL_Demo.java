package SparkStreamingDemo;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.io.Serializable;


/**
 * 您可以轻松地对流数据使用DataFrames和SQL操作。
 * 您必须使用StreamingContext正在使用的SparkContext创建SparkSession。
 * 此外，必须这样做，以便可以在驱动器故障时重新启动。
 * 这是通过创建一个延迟实例化的SparkSession单例实例来完成的。
 * 这在以下示例中显示。
 * 它修改了早期的单词计数示例，以使用DataFrames和SQL生成单词计数。
 * 每个RDD都转换为DataFrame，注册为临时表，然后使用SQL进行查询。
 */
public class Dstream_SQL_Demo {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setMaster("local[4]").setAppName("对数据流使用Dataset和SQL操作");
        JavaStreamingContext jssc=new JavaStreamingContext(conf, Durations.seconds(3));
        JavaDStream<String> word=jssc.socketTextStream("192.168.179.128",9999);
        word.foreachRDD((rdd,time)->{
         //   SparkSession spark= SparkSession.builder().config(rdd.sparkContext().getConf()).getOrCreate()
            //看SparkStreamingDemo.JavaSqlNetworkWordCount
        });
    }
}
class JavaRow implements Serializable{
    private String word;

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }
}
