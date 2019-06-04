package SparkSQLdemo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQLdemo {
    public static void main(String[] args) {
        SparkSession spark=SparkSession
                .builder()
                .appName("SparkSQL的示例")
                .getOrCreate();
        Dataset<Row> df=spark.read().json("C:\\临时文件\\JsonTest.txt");

        //将DataFrame注册为SQL临时视图
        df.createOrReplaceTempView("people");

        //使用SQL语句
        Dataset<Row> sqlDF=spark.sql("select * from people");
        sqlDF.show();

        //Spark SQL中的临时视图是会话范围的，如果创建它的会话终止，它将消失。
        // 如果您希望拥有一个在所有会话之间共享的临时视图并保持活动状态，直到Spark应用程序终止，
        // 您可以创建一个全局临时视图。全局临时视图与系统保留的数据库绑定

        //将DataFrame注册为全局临时视图
        df.createOrReplaceGlobalTempView("people");
        //全局临时视图绑定到系统保留的数据库`global_temp`
        spark.sql("select * from global_temp.people").show();
        //全局临时视图是跨会话的
        spark.newSession().sql("select * from global_temp.people").show();

    }
}
