package SparkStreamingDemo;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
/**
 * Use this singleton to get or register an Accumulator.
 * 使用此单例获取或注册累加器。
 */
public class JavaDroppedWordsCounter{
    private static volatile LongAccumulator instance=null;
    public static LongAccumulator getInstance(JavaSparkContext jsc){
        if(instance==null){
            synchronized (JavaDroppedWordsCounter.class){
                if(instance==null){
                    instance=jsc.sc().longAccumulator("WordsInBlacklistCounter");
                }
            }
        }
        return  instance;
    }
}
