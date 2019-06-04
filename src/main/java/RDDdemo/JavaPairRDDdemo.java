package RDDdemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.List;

public class JavaPairRDDdemo {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("使用键值对").setMaster("local");
        JavaSparkContext jsc=new JavaSparkContext(conf);
        JavaRDD<String> lines=jsc.textFile("hello.txt");
        JavaPairRDD<String,Integer> pairs=lines.mapToPair(s->new Tuple2<>(s,1));
        JavaPairRDD<String,Integer> counts=pairs.reduceByKey((a,b)->a+b);
        counts.sortByKey();
        List<Tuple2<String,Integer>> list=counts.collect();
        for(int i=0;i<list.size();i++){
            System.out.println(list.get(i)._1+"***"+list.get(i)._2);
        }

    }
}
