package SparkSQLdemo;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

/**
 * Spark SQL支持两种不同的方法将现有RDD转换为数据集。第一种方法使用反射来推断包含特定类型对象的RDD的模式。
 * 这种基于反射的方法可以提供更简洁的代码，并且在您编写Spark应用程序时已经了解模式时可以很好地工作。
 *
 * 创建数据集的第二种方法是通过编程接口，允许您构建模式，然后将其应用于现有RDD。虽然此方法更详细，
 * 但它允许您在直到运行时才知道列及其类型时构造数据集。
 */

/**
 * 使用反射推断模式
 */
public class InteroperatingWithRDDs {
    public static void main(String[] args) {
//Spark SQL支持自动将JavaBeans的RDD 转换为DataFrame。的BeanInfo，使用反射得到，定义了表的模式。
// 目前，Spark SQL不支持包含Map字段的JavaBean 。
// 但是支持嵌套的JavaBeans和/ List或Array字段。
// 您可以通过创建实现Serializable的类来创建JavaBean，并为其所有字段设置getter和setter。

        SparkSession spark=SparkSession.builder().appName("与RDD的交互操作").getOrCreate();
        //从文件文本创建一个Person类型的RDD对象
        JavaRDD<Person> peopleRDD=spark.read()
                .textFile("C:\\临时文件\\JsonTest.txt")
                .javaRDD()
                .map(
                  line->
                  {
                      String[] parts = line.split(",");
                        Person person=new Person();
                        person.setAge(Integer.parseInt(parts[0]));
                        person.setName(parts[1]);
                        return person;
                  }
                );

        //在RDD上应用 javaBeen的模式创建一个DataFrame
        Dataset<Row> peopleDF=spark.createDataFrame(peopleRDD,Person.class);

        //将DataFrame注册为临时视图
        peopleDF.createOrReplaceTempView("people");
        //SQL语句可以通过使用spark提供的SQL方法来运行
        Dataset<Row> teenagersDF=spark.sql("select * form people where age between 13 and 19");

        //通过字段索引访问结果中的行中的列
        Encoder<String> stringEncoder= Encoders.STRING();
        Dataset<String> teenagerEncoder=teenagersDF.map(
                (MapFunction<Row,String>)row->"name:"+row.getString(0),
                stringEncoder
        );
        teenagerEncoder.show();

        // or by field name只通过字段访问，不用索引
        Dataset<String> teenagerNamesByFieldDF = teenagersDF.map(
                (MapFunction<Row, String>) row -> "Name: " + row.<String>getAs("name"),
                stringEncoder);
        teenagerNamesByFieldDF.show();

    }
}
