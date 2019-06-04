package StructStreaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.Trigger;

/**
 * 流式查询的触发器设置定义了流式数据处理的时间，查询是作为具有固定批处理间隔的微批量查询还是作为连续处理查询来执行。
 * 以下是支持的各种触发器。
 * 触发类型	                 描述
 * 未指定（默认）	         如果未明确指定触发设置，则默认情况下，查询将以微批处理模式执行，一旦前一个微批处理完成处理，将立即生成微批处理。
 *
 * 固定间隔微批次	       查询将以微批处理模式执行，其中微批处理将以用户指定的间隔启动。
 *                          1.如果先前的微批次在该间隔内完成，则引擎将等待该间隔结束，然后开始下一个微批次。
 *                          2.如果前一个微批次需要的时间长于完成的间隔（即如果错过了间隔边界），
 *                            则下一个微批次将在前一个完成后立即开始（即，它不会等待下一个间隔边界） ）。
 *                          3.如果没有可用的新数据，则不会启动微批次。
 *
 * 一次性微批次	            查询将执行*仅一个*微批处理所有可用数据，然后自行停止。这在您希望定期启动集群，处理自上一个时间段以来可用的所有内容，然后关闭集群的方案中非常有用。在某些情况下，这可能会显着节省成本。
 *
 * 连续固定检查点间隔         * （实验）	查询将以新的低延迟，连续处理模式执行。在下面的连续处理部分中阅读更多相关信息。
 */
public class TriggerDemo {
    public static void main(String[] args) {
        SparkSession spark= SparkSession.builder().appName("触发器示例").getOrCreate();
        Dataset<Row> df=spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load();
        //默认触发器(尽快运行微批处理)
        df.writeStream()
                .format("console")
                .start();

        //ProcessingTime trigger with two-seconds micro-batch interval
        //处理时间触发器与2秒的微批处理间隔
        df.writeStream()
                .format("console")
                .trigger(Trigger.ProcessingTime("2 seconds"))
                .start();

        //One-time trigger
        //一次触发
        df.writeStream()
                .format("console")
                .trigger(Trigger.Once())
                .start();

        //Continuous trigger with one-second checkpointing interval
        //带有一秒检查点间隔的连续触发器
        df.writeStream()
                .format("console")
                .trigger(Trigger.Continuous("1 second"))
                .start();
    }
}
