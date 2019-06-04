package StructStreaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryListener;

/**
 * 有多种方法可以监控活动的流式查询。您可以使用Spark的Dropwizard Metrics支持将指标推送到外部系统，也可以通过编程方式访问它们。
 *
 * 可以使用streamingQuery.lastProgress()和直接获取活动查询的当前状态和指标 streamingQuery.status()。
 *
 * Scala 和Java中的StreamingQueryProgress对象,包含有关在流的最后一次触发中所取得进展的所有信息 - 处理了哪些数据，处理速率，延迟等等。
 *
 * streamingQuery.status()返回Scala 和Java中的StreamingQueryStatus对象,它提供了有关查询立即执行操作的信息 - 触发器是否处于活动状态，
 * 是否正在处理数据等。
 */
public interface MonitoringQueriesDemo {
    public static void main(String[] args) {
        SparkSession spark= SparkSession.builder().appName("流式查询的监视").getOrCreate();
        Dataset<Row> df=spark
                .readStream()
                .format("socket")
                .option("host","localhost")
                .option("port","9999")
                .load();

        StreamingQuery query=df.writeStream().start();
        System.out.println(query.lastProgress());
        //将会打印出类似于下面的输出
        /* Will print something like the following.

{
  "id" : "ce011fdc-8762-4dcb-84eb-a77333e28109",
  "runId" : "88e2ff94-ede0-45a8-b687-6316fbef529a",
  "name" : "MyQuery",
  "timestamp" : "2016-12-14T18:45:24.873Z",
  "numInputRows" : 10,
  "inputRowsPerSecond" : 120.0,
  "processedRowsPerSecond" : 200.0,
  "durationMs" : {
    "triggerExecution" : 3,
    "getOffset" : 2
  },
  "eventTime" : {
    "watermark" : "2016-12-14T18:45:24.873Z"
  },
  "stateOperators" : [ ],
  "sources" : [ {
    "description" : "KafkaSource[Subscribe[topic-0]]",
    "startOffset" : {
      "topic-0" : {
        "2" : 0,
        "4" : 1,
        "1" : 1,
        "3" : 1,
        "0" : 1
      }
    },
    "endOffset" : {
      "topic-0" : {
        "2" : 0,
        "4" : 115,
        "1" : 134,
        "3" : 21,
        "0" : 534
      }
    },
    "numInputRows" : 10,
    "inputRowsPerSecond" : 120.0,
    "processedRowsPerSecond" : 200.0
  } ],
  "sink" : {
    "description" : "MemorySink"
  }
}
*/

        System.out.println(query.status());
/*  Will print something like the following.
{
  "message" : "Waiting for data to arrive",
  "isDataAvailable" : false,
  "isTriggerActive" : false
}
*/


//          使用异步API以编程方式报告度量标准
//SparkSession还可以通过附加StreamingQueryListener （Scala / Java文档）异步监视与a关联的所有查询 。
// 附加自定义StreamingQueryListener对象后 sparkSession.streams.attachListener()，
// 将在启动和停止查询以及活动查询中取得进展时获得回调。

        //有问题，下面三个方法中的query应该分别是queryStarted.id()，queryTerminated.id()，queryProgress.progress()
        spark.streams().addListener(new StreamingQueryListener() {


            @Override
            public void onQueryStarted(QueryStartedEvent queryStarted) {
                System.out.println("Query started: " + queryStarted.id());            }

            @Override
            public void onQueryProgress(QueryProgressEvent queryProgress) {
                System.out.println("Query made progress: " + queryProgress.progress());
            }

            @Override
            public void onQueryTerminated(QueryTerminatedEvent queryTerminated) {
                System.out.println("Query terminated: " + queryTerminated.id());
            }
        });

    }
}
