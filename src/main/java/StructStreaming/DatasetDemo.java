package StructStreaming;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.expressions.javalang.typed;
import org.apache.spark.sql.types.StructType;

import java.sql.Date;

public class DatasetDemo {
    public static void main(String[] args) {
        SparkSession spark= SparkSession.builder().appName("Dataset和DataFrame的流式操作").getOrCreate();
        //模式{device: string, type: string, signal: double, time: DateType}
        StructType schema=new StructType().add("device","String").add("type","String").add("signal","double").add("time","DataType");
        Dataset<Row> df=spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .schema(schema)
                .load();

        // streaming Dataset with IOT device data  流数据与物联网设备数据
        Dataset<DeviceData> ds=df.as(ExpressionEncoder.javaBean(DeviceData.class));
        //查询信号大于10的设备
        df.select("device").where("signal>10");
        ds.filter((FilterFunction<DeviceData>) value -> value.getSignal() > 10)
                .map((MapFunction<DeviceData, String>) value -> value.getDevice(), Encoders.STRING());

        //Running count of the number of updates for each device type每种设备类型的更新数的运行计数
        df.groupBy("type").count();

        //运行每种设备类型的平均信号
        //Running average signal for each device type
        ds.groupByKey((MapFunction<DeviceData,String>)value -> value.getDeviceType(),Encoders.STRING())
                .agg(typed.avg((MapFunction<DeviceData, Double>) value -> value.getSignal()));

        //将流式DataFrame / Dataset注册为临时视图，然后在其上应用SQL命令。
        df.createOrReplaceTempView("updates");
        spark.sql("select count(*) from updates");
    }
}
class DeviceData{
    private String device;
    private String deviceType;
    private Double signal;
    private java.sql.Date time;

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    public String getDeviceType() {
        return deviceType;
    }

    public void setDeviceType(String deviceType) {
        this.deviceType = deviceType;
    }

    public Double getSignal() {
        return signal;
    }

    public void setSignal(Double signal) {
        this.signal = signal;
    }

    public Date getTime() {
        return time;
    }

    public void setTime(Date time) {
        this.time = time;
    }
}
