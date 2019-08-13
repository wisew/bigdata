package com.wangwei.spark.rdd

import org.apache.spark.{SparkConf, SparkContext}

object Demo extends App{
  val conf = new SparkConf().setAppName("hello").setMaster("local")
  val sc = new SparkContext(conf)
  val data = Array(("a",1),("a",2))
  val rdd = sc.parallelize(data)
  rdd.reduceByKey(_+_).collect().foreach(println)
  sc.stop()
}
