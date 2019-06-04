package KafkaDemo;

import org.apache.kafka.clients.producer.Producer;

import java.util.Properties;

public class SparkStreamingDataManuallyProducerForKafka extends Thread {
    private String topic;//发送给kafka的数据的类别
    private Producer<Integer,String> producerForKafka;
    public SparkStreamingDataManuallyProducerForKafka(String topic){
        this.topic=topic;
        Properties conf=new Properties();
       // conf.put()
    }
}
