package RDDdemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

/**
 * 广播变量允许程序员在每台机器上保留一个只读变量，而不是随副本一起发送它的副本。
 * 1. 广播变量只能在 Driver 端定义
 * 2. 广播变量在 Executor 端无法修改
 * 3. 只能在 Driver 端改变广播变量的值
 */
public class BroadCastDemo {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setMaster("local").setAppName("广播变量");
        JavaSparkContext jsc=new JavaSparkContext(conf);
        Broadcast<int[]> broadcast=jsc.broadcast(new int[]{1,2,3});
        broadcast.value();

    }
}
