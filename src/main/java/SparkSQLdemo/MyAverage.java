package SparkSQLdemo;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.MutableAggregationBuffer;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

/**
 * 该内置功能DataFrames提供共同聚合，例如count()，countDistinct()，avg()，max()，min()，等。
 *
 * 用户必须扩展UserDefinedAggregateFunction 抽象类以实现自定义无类型聚合函数。
 * 例如，用户定义的平均值可能如下所示：
 */
public class MyAverage extends UserDefinedAggregateFunction {
    private StructType inputSchema;
    private StructType bufferSchema;
    public MyAverage(){
        List<StructField> inputFields= new ArrayList<>();
        inputFields.add(DataTypes.createStructField("inputColum",DataTypes.LongType,true));
        inputSchema = DataTypes.createStructType(inputFields);

        List<StructField> bufferFields = new ArrayList<>();
        bufferFields.add(DataTypes.createStructField("sum", DataTypes.LongType, true));
        bufferFields.add(DataTypes.createStructField("count", DataTypes.LongType, true));
        bufferSchema = DataTypes.createStructType(bufferFields);

    }

   //此聚合函数的输入参数的数据类型
    @Override
    public StructType inputSchema() {
        return inputSchema;
    }

    //聚合缓冲区中值的数据类型
    @Override
    public StructType bufferSchema() {
        return bufferSchema;
    }

    //返回值的数据类型
    @Override
    public DataType dataType() {
        return DataTypes.DoubleType;
    }

    //这个函数是否总是在相同的输入上返回相同的输出
    @Override
    public boolean deterministic() {
        return true;
    }

    //初始化给定的聚合缓冲区。缓冲区本身是一个“Row”
//提供了一些标准方法，比如在索引处检索值(例如get()、getBoolean())
//有机会更新它的值。注意，缓冲区内的数组和映射仍然是吗?
     //不可变的。
    @Override
    public void initialize(MutableAggregationBuffer buffer) {
        buffer.update(0, 0L);
        buffer.update(1, 0L);
    }

    //使用来自“input”的新输入数据更新给定的聚合缓冲区“buffer”
    @Override
    public void update(MutableAggregationBuffer buffer, Row input) {
        if (!input.isNullAt(0)) {
            long updatedSum = buffer.getLong(0) + input.getLong(0);
            long updatedCount = buffer.getLong(1) + 1;
            buffer.update(0, updatedSum);
            buffer.update(1, updatedCount);
        }
    }

    //合并两个聚合缓冲区，并将更新后的缓冲区值存储回' buffer1 '
    @Override
    public void merge(MutableAggregationBuffer buffer1, Row buffer2) {
        long mergedSum = buffer1.getLong(0) + buffer2.getLong(0);
        long mergedCount = buffer1.getLong(1) + buffer2.getLong(1);
        buffer1.update(0, mergedSum);
        buffer1.update(1, mergedCount);
    }
    //计算最终结果
    @Override
    public Object evaluate(Row buffer) {
        return ((double) buffer.getLong(0)) / buffer.getLong(1);
    }

    public static void main(String[] args) {
        SparkSession spark= SparkSession.builder().appName("自定义了一个球平均值的函数").getOrCreate();
        // 注册函数来访问它
        spark.udf().register("myAverage", new MyAverage());

        Dataset<Row> df = spark.read().json("examples/src/main/resources/employees.json");
        df.createOrReplaceTempView("employees");
        df.show();
// +-------+------+
// |   name|salary|
// +-------+------+
// |Michael|  3000|
// |   Andy|  4500|
// | Justin|  3500|
// |  Berta|  4000|
// +-------+------+

        Dataset<Row> result = spark.sql("SELECT myAverage(salary) as average_salary FROM employees");
        result.show();
// +--------------+
// |average_salary|
// +--------------+
// |        3750.0|
// +--------------+
    }
}
