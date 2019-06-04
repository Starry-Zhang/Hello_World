package StructStreaming;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

/**
 * 这些示例生成无类型的流式DataFrame，这意味着在编译时不检查DataFrame的架构，
 * 仅在提交查询时在运行时检查。某些操作（如map，flatMap等）需要在编译时知道类型。
 * 要执行这些操作，您可以使用与静态DataFrame相同的方法将这些无类型流式DataFrame转换为类型化流式数据集。
 */
public class CSVdemo {
    public static void main(String[] args) {
        SparkSession spark= SparkSession.builder().appName("读取目录下的所有csv文件").getOrCreate();


        // Read text from socket
        Dataset<Row> socketDF = spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load();

        socketDF.isStreaming();    // Returns True for DataFrames that have streaming sources

        socketDF.printSchema();

        //读取目录下的所有csv文件
        StructType userSchema=new StructType().add("name","String").add("age","int");
        Dataset<Row> csvDF=spark
                .readStream()
                .option("sep",";")
                .schema(userSchema)
                .csv("C:\\临时文件\\垃圾短信过滤各省关键字表");
                //csv(path:"")等同于format("csv").load("/path/to/directory")

    }
}
