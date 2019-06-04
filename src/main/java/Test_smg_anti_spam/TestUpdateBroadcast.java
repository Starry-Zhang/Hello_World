package Test_smg_anti_spam;





import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;


import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestUpdateBroadcast {
    private static Broadcast<Map<String, List<String>>> instance=null;
    private static Map<String, List<String>> map=new HashMap<>();
    public static Map<String, List<String>> getMySQL() throws SQLException, ClassNotFoundException {
        ConstJDBC con=new ConstJDBC();
        Statement statement = con.getStatement();
        ResultSet rs = statement.executeQuery("select * from testUpdate");
        while (rs.next()){
            String id=rs.getString("id");
            String name=rs.getString("name");
            if (map.containsKey(id)) {
                map.get(id).add(name);
            } else {
                ArrayList<String> list = new ArrayList<>();
                list.add(name);
                map.put(id, list);
            }
        }
       rs.close();
        con.close();
        return map;
    }
    /*这是API上的原话
              def unpersist(blocking: Boolean): Unit
     Permalink
    Delete cached copies of this broadcast on the executors. If the broadcast is used after this is called, it will need to be re-sent to each executor.

    blocking
    Whether to block until unpersisting has completed
     */
    public static void update(JavaSparkContext jsc, boolean block) throws SQLException, ClassNotFoundException {
        if(instance!=null){
            instance.unpersist(block);
        }
        getMySQL();
        instance=jsc.broadcast(map);
    }

    public Broadcast<Map<String, List<String>>> getInstance(JavaSparkContext jsc) throws SQLException, ClassNotFoundException {
        if (instance == null) {
            synchronized (instance){
                if (instance == null) {
                    instance = jsc.broadcast(getMySQL());
                }
            }
        }
        return instance;
    }



    public static void main(String[] args) throws SQLException, ClassNotFoundException, InterruptedException {
        SparkConf conf=new SparkConf().setAppName("跟新广播变量").setMaster("local[4]");
        JavaSparkContext jsc=new JavaSparkContext(conf);
        JavaStreamingContext jssc=new JavaStreamingContext(jsc, Durations.seconds(3));
        JavaReceiverInputDStream<String> lines=jssc.socketTextStream("192.168.179.128",9999);
        JavaDStream<String> words=lines.flatMap(x-> Arrays.asList(x.split(" ")).iterator());
        words.print();
        words.foreachRDD(rdd->{
                //设计操作：广播变量的跟新和初始化
                //自定义时间
                if(new GetDate().getHour()==16){
                    update(jsc, true);
                    System.out.println("广播变量更新成功： " + new GetDate().getHour());
                }
            if(instance.value().containsKey("2")){
                System.out.println("广播变量中的key存在2");
                jssc.stop();
            }
        });

        jssc.start();              // 开始计算
        jssc.awaitTermination();
        ExecutorService executorService=Executors.newScheduledThreadPool(1);
    }

}
