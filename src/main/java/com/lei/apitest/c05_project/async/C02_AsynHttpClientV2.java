package com.lei.apitest.c05_project.async;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * @Author:
 * @Date: 2020-09-15 15:51
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class C02_AsynHttpClientV2 {

    private static AtomicInteger finishedCnt = new AtomicInteger(0);

    public static void main(String[] args) throws InterruptedException {
        // 异步请求
        long startT = System.currentTimeMillis();
        Vector<Thread> vector = new Vector<>();
        for (int index = 0; index < 3; index++) {
            MyRunThread myRunThread = new MyRunThread("threadName:" + index, 30);
            vector.add(myRunThread);
            myRunThread.start();
        }

        for (Thread thread : vector) {
            thread.join();
        }
        long endT = System.currentTimeMillis();
        long spendT = endT - startT;
        System.out.println("way 2...........spendT: " + spendT);
    }

    static class Back implements FutureCallback<HttpResponse>{

        private long start = System.currentTimeMillis();
        private CountDownLatch countDownLatch;

        Back(CountDownLatch countDownLatch){
            this.countDownLatch = countDownLatch;
        }

        public void completed(HttpResponse httpResponse) {
            try {
                if (httpResponse.getStatusLine().getStatusCode() == 200) {
                    System.out.println(" finishedCnt:" + finishedCnt.incrementAndGet());
//                    HttpEntity entity = httpResponse.getEntity();
//                    String res = EntityUtils.toString(entity);
//                    System.out.println("cost is:"+(System.currentTimeMillis()-start)+":"+ res + " finishedCnt:" + finishedCnt.incrementAndGet());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            countDownLatch.countDown();
        }

        public void failed(Exception e) {
            System.err.println(" cost is:"+(System.currentTimeMillis()-start)+":"+e);
            countDownLatch.countDown();
        }

        public void cancelled() {
            countDownLatch.countDown();
        }
    }
}


class MyRunThread extends Thread {
    private String threadName;
    private int runTimes;
    private CountDownLatch countDownLatch;

    public MyRunThread() { }

    public MyRunThread(String threadName, int runTimes) {
        this.threadName = threadName;
        this.runTimes = runTimes;
        this.countDownLatch = new CountDownLatch(runTimes);
    }

    @Override
    public void run() {

        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(500000)
                .setSocketTimeout(500000)
                .setConnectionRequestTimeout(10000)
                .build();

        // 配置io线程
        IOReactorConfig ioReactorConfig = IOReactorConfig.custom().
                setIoThreadCount(Runtime.getRuntime().availableProcessors())
                .setSoKeepAlive(true)
                .build();
        // 设置连接池大小
        ConnectingIOReactor ioReactor = null;
        try {
            ioReactor = new DefaultConnectingIOReactor(ioReactorConfig);
        } catch (IOReactorException e) {
            e.printStackTrace();
        }
        PoolingNHttpClientConnectionManager connManager = new PoolingNHttpClientConnectionManager(ioReactor);
        connManager.setMaxTotal(100);
        connManager.setDefaultMaxPerRoute(100);


        final CloseableHttpAsyncClient client = HttpAsyncClients.custom().
                setConnectionManager(connManager)
                .setDefaultRequestConfig(requestConfig)
                .build();


        // 构造请求
        String url = "https://www.cnblogs.com/";
        HttpPost httpPost = new HttpPost(url);

        // start
        client.start();

        // 异步请求
        long start = System.currentTimeMillis();
        for (int i = 0; i < this.runTimes; i++) {
            client.execute(httpPost, new C02_AsynHttpClientV2.Back(countDownLatch));
        }

        try {
            System.err.println(this.threadName + " 全部指令发送完毕");
            countDownLatch.await(); // 等待 latch1 变成0
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long end = System.currentTimeMillis();
        long spend = end - start;
        System.out.println(Thread.currentThread().getName() + " spend: " + spend);

        try {
            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}