package com.wangwei.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * transform算子
  * 分两步走
  * 第一个函数计算各自分区内部按key聚合，使用到zeroValue
  * 第二个函数对每个分区(此时分区内部key是唯一的)的key再次聚合，不会用到zeroValue
  */
object AggregateByKeyDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hello").setMaster("local[4]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
        val data = Array(("a", 1), ("a", 2), ("a", 3), ("a", 4), ("b", 1), ("b", 2), ("a", 2), ("b", 2), ("c", 2), ("d", 3))
    val rdd = sc.parallelize(data, 3).cache()
    println("glom=======")
    rdd.glom().collect().foreach(x => {
      x.foreach(print)
      println()
    })
    println("===============")
    //    val res=rdd.aggregateByKey(1)(
    //      (x:Int,y:Int)=>x+y,
    //      (x:Int,y:Int)=>x+y
    ////    验证第二步中zeroValue不参与计算
    ////      (x:Int,y:Int)=>x
    //    )
    val res = rdd.aggregateByKey((0, 0))(
      (x: Tuple2[Int, Int], y: Int) => (x._1 + y, x._2 + 1),
      (x: Tuple2[Int, Int], y: Tuple2[Int, Int]) => (x._1 + y._1, x._2 + y._2)
    )
    res.map(x => (x._1, x._2._1 / x._2._2.toDouble)).collect().foreach(println)
    sc.stop()
  }
}
