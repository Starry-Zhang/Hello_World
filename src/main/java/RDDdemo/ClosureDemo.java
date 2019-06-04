package RDDdemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 理解闭包
 */
public class ClosureDemo {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("理解闭包").setMaster("local");
        JavaSparkContext jsc=new JavaSparkContext(conf);
        AtomicInteger counter= new AtomicInteger();
//        int counter=0;这是idea没有帮助修改之前的
        List<Integer> list= Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> rdd=jsc.parallelize(list);
        rdd.foreach(x-> counter.addAndGet(x));
//        rdd.foreach(x->counter+=x);这是idea没有帮助修改之前的

    }
}
