package com.lei.apitest.c05_project;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.nio.IOControl;
import org.apache.http.nio.client.methods.AsyncCharConsumer;
import org.apache.http.nio.client.methods.HttpAsyncMethods;
import org.apache.http.nio.protocol.HttpAsyncRequestProducer;
import org.apache.http.protocol.HttpContext;

import java.io.IOException;
import java.nio.CharBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class C02_CloseableHttpAsyncTest {
    public static void main(String[] args) throws IOException {
        CloseableHttpAsyncClient httpclient = HttpAsyncClients.createDefault();
        try {
            // Start the client
            httpclient.start();

//            // Execute request
//            final HttpGet request1 = new HttpGet("http://www.apache.org/");
//            Future<HttpResponse> future = httpclient.execute(request1, null);
//            // and wait until a response is received
//            HttpResponse response1 = future.get();
//            System.out.println(request1.getRequestLine() + "->" + response1.getStatusLine());
//            System.out.println("way 1.................");

            // One most likely would want to use a callback for operation result
            final CountDownLatch latch1 = new CountDownLatch(20);
            final HttpGet request2 = new HttpGet("http://www.apache.org/");
            for (int i = 0; i < 20; i++) {
                httpclient.execute(request2, new FutureCallback<HttpResponse>() {

                    public void completed(final HttpResponse response2) {
                        latch1.countDown();
                        System.out.println(request2.getRequestLine() + "->" + response2.getStatusLine());
                    }

                    public void failed(final Exception ex) {
                        latch1.countDown();
                        System.out.println(request2.getRequestLine() + "->" + ex);
                    }

                    public void cancelled() {
                        latch1.countDown();
                        System.out.println(request2.getRequestLine() + " cancelled");
                    }

                });
            }

            latch1.await(); // 等待 latch1 变成0
            System.out.println("way 2...........");

            // In real world one most likely would also want to stream
            // request and response body content
//            final CountDownLatch latch2 = new CountDownLatch(1);
//            final HttpGet request3 = new HttpGet("http://www.apache.org/");
//            HttpAsyncRequestProducer producer3 = HttpAsyncMethods.create(request3);
//            AsyncCharConsumer<HttpResponse> consumer3 = new AsyncCharConsumer<HttpResponse>() {
//
//                HttpResponse response;
//
//                @Override
//                protected void onResponseReceived(final HttpResponse response) {
//                    this.response = response;
//                }
//
//                @Override
//                protected void onCharReceived(final CharBuffer buf, final IOControl ioctrl) throws IOException {
//                    // Do something useful
//                }
//
//                @Override
//                protected void releaseResources() {
//                }
//
//                @Override
//                protected HttpResponse buildResult(final HttpContext context) {
//                    return this.response;
//                }
//
//            };
//            httpclient.execute(producer3, consumer3, new FutureCallback<HttpResponse>() {
//
//                public void completed(final HttpResponse response3) {
//                    latch2.countDown();
//                    System.out.println(request3.getRequestLine() + "->" + response3.getStatusLine());
//                }
//
//                public void failed(final Exception ex) {
//                    latch2.countDown();
//                    System.out.println(request3.getRequestLine() + "->" + ex);
//                }
//
//                public void cancelled() {
//                    latch2.countDown();
//                    System.out.println(request3.getRequestLine() + " cancelled");
//                }
//
//            });
//            latch2.await();
//            System.out.println("way3 .....................");
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            httpclient.close();
        }
    }
}
