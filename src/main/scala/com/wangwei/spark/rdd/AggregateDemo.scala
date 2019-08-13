package com.wangwei.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * aggregate为action算子
  * 1~10求和
  * 分两步走
  * 第一个函数计算各自分区内部聚合的值，使用到zeroValue
  * 第二个函数对每个分区聚合的值再次聚合，也使用到zeroValue
  */
object AggregateDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName(AggregateDemo.getClass.getName).setMaster("local[4]")
    val sc = new SparkContext(conf)
    val data = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val rdd = sc.parallelize(data).cache()
    println("glom=====")
    rdd.glom().collect().foreach(x => {
      x.foreach(x => print(x + ","))
      println()
    })
    val res = rdd.aggregate(1)(
      (zeroValue, item) => zeroValue + item,
      (zeroValue, item) => item + zeroValue
      // 可以去掉注释验证函数分两步走的细节
//      (zeroValue, item) => item
    )
    println(res)
    sc.stop()
  }
}
