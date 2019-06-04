package RDDdemo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;

import java.util.Arrays;
import java.util.List;

public class FirstDemo {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("firstDemo").setMaster("local");
        JavaSparkContext sc=new JavaSparkContext(conf);
        //并行化 驱动程序中的现有集合创建RDD
        List<Integer> list= Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> distData0=sc.parallelize(list);
        //文本文件RDDS可以使用创建SparkContext的textFile方法
        JavaRDD<String> distData1=sc.textFile("hello.txt");
       // sc.wholeTextFiles("");//允许您读取包含多个小文本文件的目录，并将它们作为（文件名，内容）对返回。
                                    //这与之相反textFile，它将在每个文件中每行返回一条记录。
        JavaRDD<Integer> lineLength= distData1.map(s -> s.length());
        int totalLength=lineLength.reduce((a,b)->a+b);
        System.out.println(totalLength);

        //将lineLengths在第一次计算后保存在内存中。
        lineLength.persist(StorageLevel.MEMORY_ONLY());

        //Spark会自动监视每个节点上的缓存使用情况，并以最近最少使用（LRU）的方式删除旧数据分区。
        // 如果您想手动删除RDD而不是等待它退出缓存，RDD.unpersist()
        lineLength.unpersist();

        //把lambda表达式换成匿名内部类
        JavaRDD<Integer> lineLength1=distData1.map(new Function<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception {
                return s.length();
            }
        });

        //用内联函数编写
        JavaRDD<Integer> lineLength2=distData1.map(new GetLength());

        //


    }
    static class GetLength implements Function<String,Integer>{

        @Override
        public Integer call(String s) throws Exception {
            return s.length();
        }
    }
}
