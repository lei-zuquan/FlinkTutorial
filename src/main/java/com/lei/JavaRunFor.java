package com.lei;

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-06-03 9:46
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
public class JavaRunFor {
    public static void main(String[] args) {
        long start =  System.currentTimeMillis();

        int t = 0;
        for (int i= 0; i<= 100000;i++) {
            t += i;
        }
        long end = System.currentTimeMillis();
        System.out.println(end-start);
        System.out.println(t);
        // 1
        // 705082704
        //scala中的for比while循环慢很多。在代码优化中可以想到在此优化。

        //还有其他的测试总结： 用java代码和scala代码，对比同一个算法，发现java比scala快很多。执行的快慢应该主要看scala编译成字节码的质量了。
    }
}
