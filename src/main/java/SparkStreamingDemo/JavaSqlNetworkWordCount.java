package SparkStreamingDemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import java.util.Arrays;
import java.util.regex.Pattern;

public class JavaSqlNetworkWordCount {
    private static final Pattern SPACE=Pattern.compile(" ");
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf=new SparkConf().setAppName("GitHub对流处理进行Dataset和SQL操作的示例");
        JavaStreamingContext jssc=new JavaStreamingContext(conf, Durations.seconds(3));
        JavaReceiverInputDStream<String> lines=jssc.socketTextStream("192.168.179.128",9999);
        JavaDStream<String> words=lines.flatMap(x-> Arrays.asList(SPACE.split(x)).iterator());
       //将单词DStream的RDDs转换为DataFrame并运行SQL查询
        words.foreachRDD((rdd,time) -> {
           // SparkSession spark=SparkSession.builder().config(rdd.context().getConf()).getOrCreate();
            SparkSession spark=JavaSparkSessionSingleton.getInstance(rdd.context().getConf());
            JavaRDD<JavaRecord> rowRDD=rdd.map(word->{
               JavaRecord record=new JavaRecord();
               record.setWord(word);
               return record;
            });
            Dataset<Row> wordsDataFrame=spark.createDataFrame(rowRDD,JavaRecord.class);

            wordsDataFrame.createOrReplaceTempView("words");
            Dataset<Row> wordCountsDataFrame =
                    spark.sql("select word, count(*) as total from words group by word");
            System.out.println("========= " + time + "=========");
            wordCountsDataFrame.show();
        });
        jssc.start();
        jssc.awaitTermination();

    }

}

class JavaSparkSessionSingleton {
    private static transient SparkSession instance = null;
    public static SparkSession getInstance(SparkConf sparkConf) {
        if (instance == null) {
            instance = SparkSession
                    .builder()
                    .config(sparkConf)
                    .getOrCreate();
        }
        return instance;
    }
}
class JavaRecord{
    private String word;

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }
}
