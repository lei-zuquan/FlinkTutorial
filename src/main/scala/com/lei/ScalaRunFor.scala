package com.lei

/**
 * @Author: Lei
 * @E-mail: 843291011@qq.com
 * @Date: 2020-06-03 9:47
 * @Version: 1.0
 * @Modified By:
 * @Description:
 */
object ScalaRunFor {
  def main(args: Array[String]): Unit = {
    // forCry
    // 177
    // 705082704

    whileCry
    // 2
    // 705082704
    //scala中的for比while循环慢很多。在代码优化中可以想到在此优化。

    //还有其他的测试总结： 用java代码和scala代码，对比同一个算法，发现java比scala快很多。执行的快慢应该主要看scala编译成字节码的质量了。
  }

  def forCry(): Unit = {
    var start = System.currentTimeMillis
    var t = 0
    for (i <- 0 to  100000) {
      t += i
    }
    val end = System.currentTimeMillis
    println(end-start)
    println(t)
  }

  def whileCry(): Unit ={
    val start = System.currentTimeMillis
    var total = 0
    var i = 0
    while ( {
      i < 100000
    }) {
      i = i + 1
      total += i
    }
    val end = System.currentTimeMillis
    println(end - start)
    println(total)
  }
}
