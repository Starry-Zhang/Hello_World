package SparkStreamingDemo;


import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

/**
 * 自定义接收器
 *
 * 以下是通过套接字接收文本流的自定义接收器。
 * 它将文本流中的'\ n'分隔行视为记录，并使用Spark存储它们。
 * 如果接收线程连接或接收有任何错误，则重新启动接收器以再次尝试连接。
 */
public class MyReceiver extends Receiver<String> {
    String host=null;
    int port=-1;


    public MyReceiver(String host_,int port_) {
        super(StorageLevel.MEMORY_AND_DISK_2());
        host=host_;
        port=port_;
    }

    @Override
    public void onStart() {//要开始接收数据的事情。
                           //onStart()会启动负责接收数据的线程。

        //启动通过连接接收数据的线程
        new Thread(this::onStart).start();

        //收到数据后，可以通过调用将数据存储在Spark中store(data)，这是Receiver类提供的方法。
        // 有许多风格store()允许一次存储接收到的数据记录，或者作为对象/序列化字节的整个集合。
        // 请注意，store()用于实现接收器的风格 会影响其可靠性和容错语义。

        //接收线程还可以使用isStopped()一种Receiver方法来检查它们是否应该停止接收数据。

    }

    @Override
    public void onStop() {//要停止接收数据的事情。
                          //onStop()确保停止接收数据的这些线程。

        //当线程调用receive()时，不需要做很多事情
        //被设计成在isStopped()返回false时自动停止

    }
    /** 创建套接字连接并接收数据，直到接收器停止*/
    private void receive() {
        Socket socket = null;
        String userInput = null;

        try {
            // connect to the server
            socket = new Socket(host, port);

            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));

            // 直到停止或连接中断，继续读取
            while (!isStopped() && (userInput = reader.readLine()) != null) {
                System.out.println("Received data '" + userInput + "'");
                store(userInput);
            }
            reader.close();
            socket.close();

            // 重新启动，以便在服务器再次激活时重新连接
            restart("Trying to connect again");
        } catch(ConnectException ce) {
            // restart if could not connect to server
            restart("Could not connect", ce);
        } catch(Throwable t) {
            // restart if there is any other error
            restart("Error receiving data", t);
        }
    }
}
