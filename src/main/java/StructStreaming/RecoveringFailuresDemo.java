package StructStreaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

/**
 * 如果发生故障或故意关机，您可以恢复先前查询的先前进度和状态，并从中断处继续。
 * 这是使用检查点和预写日志完成的。您可以使用检查点位置配置查询，查询将保存所
 * 有进度信息（即每个触发器中处理的偏移范围）和运行聚合（例如快速示例中的字数）
 * 到检查点位置。此检查点位置必须是HDFS兼容文件系统中的路径，并且可以在启动查询
 * 时设置为DataStreamWriter中的选项。
 */
public class RecoveringFailuresDemo {
    public static void main(String[] args) {
        SparkSession spark= SparkSession.builder().appName("通过检查点回复故障").getOrCreate();
        Dataset<Row> df=spark
                .readStream()
                .format("socket")
                .option("host","localhost")
                .option("port","9999")
                .load();

        StreamingQuery query=df
                .writeStream()
                .outputMode("complete")
                .option("checkpointLocation","path/to/HDFS/dir")
                .format("memory")
                .start();

    }
}
