package StructStreaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.expr;

public class Join_Water {
    public static void main(String[] args) {
        SparkSession spark= SparkSession.builder().appName("流与流之间带水印的连接").getOrCreate();
        Dataset<Row> impressions = spark.readStream().load();
        Dataset<Row> clicks = spark.readStream().load();

// Apply watermarks on event-time columns
        Dataset<Row> impressionsWithWatermark = impressions.withWatermark("impressionTime", "2 hours");
        Dataset<Row> clicksWithWatermark = clicks.withWatermark("clickTime", "3 hours");

// Join with event-time constraints
        impressionsWithWatermark.join(
                clicksWithWatermark,
                expr(
                        "clickAdId = impressionAdId AND " +
                                "clickTime >= impressionTime AND " +
                                "clickTime <= impressionTime + interval 1 hour ")
        );

        //带谁赢的外部连接
        impressionsWithWatermark.join(
                clicksWithWatermark,
                expr(
                        "clickAdId = impressionAdId AND " +
                                "clickTime >= impressionTime AND " +
                                "clickTime <= impressionTime + interval 1 hour "),
                "leftOuter"                 // can be "inner", "leftOuter", "rightOuter"
        );
    }
}
