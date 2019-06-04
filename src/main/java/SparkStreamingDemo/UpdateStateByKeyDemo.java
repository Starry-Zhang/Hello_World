package SparkStreamingDemo;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;


import java.util.Arrays;
import java.util.List;


public class UpdateStateByKeyDemo {
    public static void main(String[] args) throws InterruptedException {
        SparkConf conf=new SparkConf().setMaster("local[2]").setAppName("updateStateByKey示例");
        JavaStreamingContext jssc=new JavaStreamingContext(conf, Durations.seconds(5));
       // JavaReceiverInputDStream<String> lines=jssc.socketTextStream("localhost",9999);
        JavaDStream<String> lines=jssc.textFileStream("C:\\临时文件\\testSparkStreaming");
        JavaDStream<String> words=lines.flatMap(x-> Arrays.asList(x.split(" ")).iterator());
        JavaPairDStream<String,Integer> pairs=words.mapToPair(s->new Tuple2<>(s,1));
        JavaPairDStream<String,Integer> wordCounts=pairs.reduceByKey((i1,i2)->i1+i2);

        jssc.checkpoint(".");

        //updateStateByKey
        Function2<List<Integer>, Optional<Integer>,Optional<Integer>> updateFunction=
                (values,state)->{
                    Integer newSum=0;
                    if(state.isPresent()){
                        newSum=state.get();
                    }
                    for(Integer value:values){
                        newSum+=value;
                    }
                    return Optional.of(newSum);
                };


        JavaPairDStream<String, Integer> runningCounts = pairs.updateStateByKey(updateFunction);

        wordCounts.print();
        jssc.start();
        jssc.awaitTermination();
    }
}
