package StructStreaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import static org.apache.spark.sql.functions.expr;


/**
 * 自Spark 2.0引入以来，Structured Streaming支持流和静态DataFrame / Dataset之间的连接（内连接和某种类型的外连接）。
 *
 * 流静态连接不是有状态的，因此不需要进行状态管理。但是，尚不支持几种类型的流静态外连接。
 */
public class Stream_Static_Joins_Demo {
    public static void main(String[] args) {
        SparkSession spark= SparkSession.builder().appName("和静态Dataset/DataFrame之间的连接").getOrCreate();
        StructType schema=new StructType().add("timestamp","Timestamp").add("type","String");
        Dataset<Row> staticDf=spark.read().schema(schema).json("//");
        Dataset<Row> streamingDf=spark
                .readStream()
                .format("socket")
                .option("host","localhost")
                .option("port","9999")
                .schema(schema)
                .load();

        //inner equi-join with a static DF
        //内部等连接与静态DF
        streamingDf.join(staticDf, "type");

        //right outer join with a static DF-----不知道为什么老是报错
    streamingDf.join(staticDf,expr("type"),"right_type");
            //上面的不太确定对不对，原文是这样的streamingDf.join(staticDf, "type", "right_join");但老是报错。



    }
}
