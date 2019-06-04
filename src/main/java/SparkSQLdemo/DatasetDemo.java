package SparkSQLdemo;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.io.Serializable;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;

/**
 * 数据集与RDD类似，但是，它们不使用Java序列化或Kryo，而是使用专用的编码器来序列化对象
 * 以便通过网络进行处理或传输。虽然编码器和标准序列化都负责将对象转换为字节，但编码器是
 * 动态生成的代码，并使用一种格式，允许Spark执行许多操作，如过滤，排序和散列，而无需将
 * 字节反序列化为对象。
 */
public class DatasetDemo {
    public static void main(String[] args) {
        SparkSession spark=SparkSession
                .builder()
                .appName("Dataset的示例")
                .getOrCreate();
//创建Person的实例对象
        Person person=new Person();
        person.setAge(18);
        person.setName("Starry");

        //为javaBean创建编码器
        Encoder<Person> personEncoder=Encoders.bean(Person.class);
       //创建Dataset
        Dataset<Person> javaBeanDS=spark.createDataset(
                Collections.singletonList(person),
                personEncoder
        );
        javaBeanDS.show();


        //类编码器中提供了大多数常见类型的编码器
        Encoder<Integer> integerEncoder=Encoders.INT();
        Dataset<Integer> primitiveDS=spark.createDataset(Arrays.asList(1,2,3,4,5),integerEncoder);

        //转换
        Dataset<Integer> transformeDS=primitiveDS.map(
                (MapFunction<Integer, Integer>)value->value+1,
                integerEncoder
        );

        transformeDS.collect();//Returns [2,3,4,5,6]

        //通过提供一个类，可以将数据流转换为数据集。基于名称的映射
        String path="C:\\临时文件\\JsonTest.txt";
        Dataset<Person> personDataset=spark.read().json(path).as(personEncoder);
        personDataset.show();


    }

}
