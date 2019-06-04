package StructStreaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DropDemo {
    public static void main(String[] args) {
        SparkSession spark=SparkSession.builder().appName("流式重复数据删除").getOrCreate();
        Dataset<Row> streamingDf=spark.readStream().load();
        // Without watermark using guid column
        streamingDf.dropDuplicates("guid");

// With watermark using guid and eventTime columns
        streamingDf
                .withWatermark("eventTime", "10 seconds")
                .dropDuplicates("guid", "eventTime");
    }
}
