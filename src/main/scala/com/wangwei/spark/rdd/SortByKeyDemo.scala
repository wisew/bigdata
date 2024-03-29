package com.wangwei.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

/**
  * sortByKey和sortBy都是全局有序的
  */
object SortByKeyDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hello").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val data = Array(("a",1),("a",2),("a",3),("a",4),("b",1),("b",2),("a",2),("b",2),("c",2),("d",3))
    val rdd = sc.parallelize(data,3)
    println("glom=======")
    rdd.glom().collect().foreach(x => {
      x.foreach(print)
      println()
    })
    val res=rdd.sortByKey()
    res.collect().foreach(println)
    sc.stop()
  }
}
