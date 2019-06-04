package StructStreaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructType;

public class WaterMarkDemo {
    public static void main(String[] args) {
        SparkSession spark= SparkSession.builder().appName("处理延迟数据和水印").getOrCreate();
        //streaming DataFrame of schema { timestamp: Timestamp, word: String }
        StructType schema=new StructType().add("timestamp","Timestamp").add("word","String");
        Dataset<Row> words=spark
                .readStream()
                .format("socket")
                .option("host","localhost")
                .option("port","9999")
                .schema(schema)
                .load();
        //将数据按窗口和单词分组，并计算每组的计数
        Dataset<Row> windowedCount=words
                .withWatermark("timestamp", "10 minutes")//设置水印
                .groupBy(
                functions.window(words.col("timestamp"),"10 minutes","5 minutes"),
                words.col("word")
        ).count();
    }
}
