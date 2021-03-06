package SparkStreamingDemo;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 *根据一个或多个许可给Apache Software Foundation（ASF）
 *贡献者许可协议。请参阅随附的NOTICE文件
 *此工作有关版权所有权的其他信息。
 * ASF根据Apache许可证2.0版将此文件许可给您
 *（“许可证”）; 除非符合规定，否则您不得使用此文件
 *许可证。您可以在以下位置获取许可证副本
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 *除非适用法律要求或书面同意，否则软件
 *根据许可证分发的“按现状”分发，
 *不附带任何明示或暗示的保证或条件。
 *有关管理权限的特定语言，请参阅许可证
 *许可证下的限制。
 */

//package org.apache.spark.examples.streaming;

import com.google.common.io.Closeables;

import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.receiver.Receiver;
import scala.Tuple2;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Custom Receiver that receives data over a socket. Received bytes is interpreted as
 * text and \n delimited lines are considered as records. They are then counted and printed.
 *
 * Usage: JavaCustomReceiver <master> <hostname> <port>
 *   <master> is the Spark master URL. In local mode, <master> should be 'local[n]' with n > 1.
 *   <hostname> and <port> of the TCP server that Spark Streaming would connect to receive data.
 *
 * To run this on your local machine, you need to first run a Netcat server
 *    `$ nc -lk 9999`
 * and then run the example
 *    `$ bin/run-example org.apache.spark.examples.streaming.JavaCustomReceiver localhost 9999`
 *
 */
/*
 *通过套接字接收数据的自定义接收器。接收的字节被解释为
 * text和\ n分隔的行被视为记录。然后对它们进行计数和打印。
 *
 *用法：JavaCustomReceiver <master> <hostname> <port>
 * <master>是Spark主URL。在本地模式下，<master>应为'local [n]'，n> 1。
 * Spark Streaming将连接以接收数据的TCP服务器的<hostname>和<port>。
 *
 *要在本地计算机上运行此命令，首先需要运行Netcat服务器
 *`$ nc -lk 9999`
 *然后运行示例
 *`$ bin / run-example org.apache.spark.examples.streaming.JavaCustomReceiver localhost 9999`
 */


public class JavaCustomReceiver extends Receiver<String> {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: JavaCustomReceiver <hostname> <port>");
            System.exit(1);
        }

   //没有-----》  StreamingExamples.setStreamingLogLevels();

        // Create the context with a 1 second batch size
        //创建1秒批量大小的上下文
        SparkConf sparkConf = new SparkConf().setAppName("JavaCustomReceiver");
        JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(1000));

        // Create an input stream with the custom receiver on target ip:port and count the
        // words in input stream of \n delimited text (eg. generated by 'nc')
        //使用目标ip：port上的自定义接收器创建输入流并计算
        // \ n分隔文本的输入流中的单词（例如，由'nc'生成）
        JavaReceiverInputDStream<String> lines = ssc.receiverStream(
                new JavaCustomReceiver(args[0], Integer.parseInt(args[1])));
        JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
        JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
                .reduceByKey((i1, i2) -> i1 + i2);

        wordCounts.print();
        ssc.start();
        ssc.awaitTermination();
    }

    // ============= Receiver code that receives data over a socket ==============
    // =============通过套接字接收数据的接收器代码==============

    String host = null;
    int port = -1;

    public JavaCustomReceiver(String host_ , int port_) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        host = host_;
        port = port_;
    }

    @Override
    public void onStart() {
        // Start the thread that receives data over a connection
        //启动通过连接接收数据的线程
        new Thread(this::receive).start();
    }

    @Override
    public void onStop() {
        // There is nothing much to do as the thread calling receive()
        // is designed to stop by itself isStopped() returns false
        //调用receive（）的线程没什么可做的
        //        //设计为自行停止isStopped（）返回false
    }

    /** Create a socket connection and receive data until receiver is stopped */
      /* *创建套接字连接并接收数据，直到接收器停止* */
    private void receive() {
        try {
            Socket socket = null;
            BufferedReader reader = null;
            try {
                // connect to the server
                socket = new Socket(host, port);
                reader = new BufferedReader(
                        new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                // Until stopped or connection broken continue reading
                String userInput;
                while (!isStopped() && (userInput = reader.readLine()) != null) {
                    System.out.println("Received data '" + userInput + "'");
                    store(userInput);
                }
            } finally {
                Closeables.close(reader, /* swallowIOException = */ true);
                Closeables.close(socket,  /* swallowIOException = */ true);
            }
            // Restart in an attempt to connect again when server is active again
            //当服务器再次处于活动状态时，重新尝试再次连接
            restart("Trying to connect again");
        } catch(ConnectException ce) {
            // restart if could not connect to server
            //重启如果无法连接到服务器
            restart("Could not connect", ce);
        } catch(Throwable t) {
            restart("Error receiving data", t);
        }
    }
}