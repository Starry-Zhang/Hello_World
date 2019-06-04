package SparkSQLdemo;

/**
 * 如果无法提前定义JavaBean类（例如，记录的结构以字符串形式编码，或者文本数据集将被解析，
 * 并且字段将针对不同的用户进行不同的投影），Dataset<Row>则可以通过三个步骤以编程方式创建a 。
 * 1.从原始RDD创建行RDD;
 * 2.创建由StructType表示的模式，该结构类型与步骤1中创建的RDD中的行结构匹配。
 * 3.通过SparkSession提供的createDataFrame方法将模式应用于行RDD。
 */

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * 以编程方式指定架构
 */
public class InteroperatingWithRDDs2 {
    public static void main(String[] args) {
        SparkSession spark= SparkSession.builder().appName("已编程的方式制定架构").getOrCreate();
        //创建RDD
        JavaRDD<String> peopleRDD=spark.sparkContext()
                .textFile("C:\\临时文件\\JsonTest.txt",1)
                .toJavaRDD();

        //用String类型来编码schema
        String schemaString="name age";
        //根据模式的字符串生成模式
        List<StructField> fields=new ArrayList<>();
        for(String fieldsName:schemaString.split(" ")){
            StructField field= DataTypes.createStructField(fieldsName, DataTypes.StringType,true);
            fields.add(field);
        }
        StructType schema=DataTypes.createStructType(fields);

        //将RDD(人员)的记录转换为行
        JavaRDD<Row> rowRDD=peopleRDD.map((Function<String,Row>)record->
        {
           String [] attributes=record.split(",");
           return RowFactory.create(attributes[0],attributes[1].trim());
        });

        //将schema应用于RDD
        Dataset<Row> peopleDataFrame=spark.createDataFrame(rowRDD,schema);

        peopleDataFrame.createOrReplaceTempView("people");
        Dataset<Row> result=spark.sql("select * from people");

        //SQL查询的结果是DataFrames并支持所有正常的RDD操作
        //结果中的行列可以通过字段索引或字段名称访问
        Dataset<String> nameDS=result.map(
                (MapFunction<Row,String>)row-> "name:"+row.getString(0),
                Encoders.STRING()
        );
        nameDS.show();
// +-------------+
// |        value|
// +-------------+
// |Name: Michael|
// |   Name: Andy|
// | Name: Justin|
// +-------------+


    }
}
