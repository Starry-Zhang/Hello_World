package SparkSQLdemo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;

public class FirstSQLdemo {
    public static void main(String[] args) {
        //Spark中所有功能的入口点都是SparkSession类。要创建基本的SparkSession，只需使用SparkSession.builder()：
        SparkSession spark=SparkSession
                .builder()
                .appName("第一个SparkSQL示例")
                .getOrCreate();

        //创建DataFrame
        Dataset<Row> df=spark.read().json("C:\\临时文件\\JsonTest.txt");
        df.show();

        //打印格式
        df.printSchema();

        //查找“name”的列
        df.select("name").show();

        //显示所有人，并把名称加1
        df.select(col("name"),col("age").plus(1)).show();

        //选择年龄超过21
        df.filter(col("age").gt(21)).show();

        //按年龄计数
        df.groupBy("age").count().show();

    }
}
