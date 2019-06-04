package StructStreaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.util.UUID;

/**
 * StreamingQuery启动查询时创建的对象可用于监视和管理查询。
 *
 * 可以在单个SparkSession中启动任意数量的查询。它们将同时运行，共享群集资源。
 * 您可以使用sparkSession.streams()来获取StreamingQueryManager （scala的/ Java的 / Python的文档），可用于管理当前活动查询。
 */
public class QueryDemo {
    public static void main(String[] args) {
        SparkSession spark= SparkSession.builder().appName("流式查询的管理").getOrCreate();
        Dataset<Row> df=spark.readStream()
                .format("socket")
                .option("host","localhost")
                .option("port","9999")
                .load();

        //获得一个查询对象
        StreamingQuery query=df.writeStream().format("console").start();

        //获取从检查点数据重新启动时持久运行的查询的惟一标识符
        UUID a=query.id();
        //获取此查询运行时的惟一id，该id将在每次启动/重启时生成
        UUID b=query.runId();
        //获取自动生成的或用户指定的名称
        String name=query.name();
        //打印查询的详细说明
        query.explain();
        //stop the query
        query.stop();
        //块，直到使用stop()或错误终止查询
        try {
            query.awaitTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }
        //如果查询已被错误终止，则为异常
        query.exception();
        //此查询的最新进展更新的数组
        query.recentProgress();
        //此流查询的最新进展更新
        query.lastProgress();


        //您可以在单个SparkSession中启动任意数量的查询。它们将同时运行，共享群集资源。
        //获取当前活动流查询的列表
        spark.streams().active();
        //通过查询对象的惟一id获取查询对象
        spark.streams().get("某个查询的id");
        //阻塞，直到其中任何一个终止
        try {
            spark.streams().awaitAnyTermination();
        } catch (StreamingQueryException e) {
            e.printStackTrace();
        }




    }
}
