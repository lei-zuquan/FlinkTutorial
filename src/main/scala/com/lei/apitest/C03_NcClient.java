package com.lei.apitest;

import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: Created in 5:30 下午 2020/4/21
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */

/**
 * 模拟nc -lk 7777 输入，达到自动精确控制控制数据
 *
 */


public class C03_NcClient {

        // 定义端口号
        private static final int PORT = 7777;

        public static void main(String[] args) throws IOException, InterruptedException {

            ServerSocket server = new ServerSocket(PORT);
            Socket socket = server.accept();

            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            System.out.println("[" + simpleDateFormat.format(new Date()) + "]" + socket.getInetAddress() + "已建立连接！");

            //监控连接是否断开线程
            //new Thread(new CheckClientThread(socket)).start();

            //输出流
            OutputStream outputStream = socket.getOutputStream();

            //控制台输入
            //Scanner in = new Scanner(System.in);

//            while (true) {
//                String str = in.nextLine() + "\n";
//
//                outputStream.write(str.getBytes());
//                outputStream.flush();
//            }
            for (int i = 11; i <= 50; i++) {
                String value = "sensor_1, 15477181" + i + ", " + (i) + "\n";
                outputStream.write(value.getBytes());
                TimeUnit.MILLISECONDS.sleep(500);
                outputStream.flush();
            }
        }

        // 监控连接程序是否断开 线程类
        static class CheckClientThread implements Runnable {

            private Socket socketClient;

            public CheckClientThread(Socket socketClient) {
                this.socketClient = socketClient;
            }

            @Override
            public void run() {
                while (true) {
                    try {
                        socketClient.sendUrgentData(0xFF);
                    } catch (IOException e) {
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        System.out.println("[" + simpleDateFormat.format(new Date()) + "]" + socketClient.getInetAddress() + "连接已关闭！");
                        // 断开后退出程序
                        System.exit(0);
                    }
                }
            }
        }
}
