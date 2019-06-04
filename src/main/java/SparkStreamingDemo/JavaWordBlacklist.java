package SparkStreamingDemo;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;
import java.util.List;

/**
 * Use this singleton to get or register a Broadcast variable.
 * 使用此单例来获得或注册广播变量
 */
public class JavaWordBlacklist{
    private static volatile Broadcast<List<String>> instance=null;
    public static Broadcast<List<String>> getInstance(JavaSparkContext jsc){
        if(instance==null){
            synchronized (JavaWordBlacklist.class){
                if(instance==null){
                    List<String> wordBlacklist= Arrays.asList("a","b","c");
                    instance=jsc.broadcast(wordBlacklist);
                }
            }
        }
        return instance;
    }
}