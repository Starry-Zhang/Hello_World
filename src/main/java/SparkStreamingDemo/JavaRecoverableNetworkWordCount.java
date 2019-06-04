package SparkStreamingDemo;

import com.google.common.io.Files;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class JavaRecoverableNetworkWordCount {
    private static final Pattern SPACE=Pattern.compile(" ");
    private static JavaStreamingContext createContext(String ip,int port,String checkpointDirectory,String outputPath){
        //如果您没有看到打印出来，这意味着StreamingContext已经加载
        //从新的检查站
        System.out.println("创建一个新的contex");
        File outputFile=new File(outputPath);
        if(outputFile.exists()){
            outputFile.delete();
        }
        SparkConf conf=new SparkConf().setAppName("检查点应用示例");
        JavaStreamingContext jssc=new JavaStreamingContext(conf, Durations.seconds(1));
        jssc.checkpoint(checkpointDirectory);
        JavaReceiverInputDStream<String> lines=jssc.socketTextStream(ip,port);
        JavaDStream<String> words=lines.flatMap(x-> Arrays.asList(SPACE.split(x)).iterator());
        JavaPairDStream<String,Integer> wordCounts=words.mapToPair(s->new Tuple2<>(s,1)).reduceByKey((i1,i2)->i1+i2);
        wordCounts.foreachRDD((rdd,time)->{
            Broadcast<List<String>> blacklist=JavaWordBlacklist.getInstance(new JavaSparkContext(rdd.context()));
            LongAccumulator droppedWordsCounter=JavaDroppedWordsCounter.getInstance(new JavaSparkContext(rdd.context()));
            String counts=rdd.filter(wordCount->{
               if(blacklist.value().contains(wordCount._1)){
                   droppedWordsCounter.add(wordCount._2);
                   return false;
               }else{
                   return true;
               }
            }).collect().toString();
            String output="Counts at time"+time+" "+counts;
            System.out.println(output);
            System.out.println("Dropped"+droppedWordsCounter.value()+"word(s) totally");
            System.out.println("Appending to"+outputFile.getAbsolutePath());//getAbsolutePath()返回绝对路径
            Files.append(output+"\n",outputFile, Charset.defaultCharset());
        });
        return jssc;
    }

    public static void main(String[] args) throws InterruptedException {
        String ip="192.168.179.128";
        int port=9999;
        String checkpointDirectory="C:\\IDEA_workSpace\\againDemo\\checkpointDir";
        String outputPath="C:\\IDEA_workSpace\\againDemo\\outputPath";
      //  函数的作用是:在不进行任何输出操作的情况下创建JavaStreamingContext
     //(用于检测新上下文)
        Function0<JavaStreamingContext> createContextFunc=()->createContext(ip,port,checkpointDirectory,outputPath);

        JavaStreamingContext jssc=JavaStreamingContext.getOrCreate(checkpointDirectory,createContextFunc);
        jssc.start();
        jssc.awaitTermination();
    }
}


