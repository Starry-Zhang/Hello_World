package RDDdemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import java.util.Arrays;

/**
 * 1. 累加器定义在 Driver 端
 * 2. Executor 端只能对累加器进行操作，也就是只能累加
 * 3. Driver 端可以读取累加器的值，Executor 端不能读取累加器的值
 *
 * 可以通过分别调用SparkContext.longAccumulator()或SparkContext.doubleAccumulator() 累积Long或Double类型的值
 * 来创建数字累加器。然后，可以使用该add方法将群集上运行的任务添加到群集中。但是，他们无法读懂它的价值。
 * 只有驱动程序可以使用其value方法读取累加器的值。
 */
public class AccumulatorDemo {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setMaster("local").setAppName("累加器");
        JavaSparkContext jsc=new JavaSparkContext(conf);
        LongAccumulator accum=jsc.sc().longAccumulator();
        jsc.parallelize(Arrays.asList(1,2,3,4,5)).foreach(x->accum.add(x));
    }
}
