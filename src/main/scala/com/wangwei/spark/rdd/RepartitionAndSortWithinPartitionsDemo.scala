package com.wangwei.spark.rdd


import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
  * 如果需要repartition，然后排序
  * 可以考虑reparitionAndSortWithinPartitions代替
  * 必须应用在(k,v)类型的rdd上
  * 只能保证分区内部有序
  */
object RepartitionAndSortWithinPartitionsDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hello").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val data = Array("abc abcd ab abcdefg abcde","acadasdad asdad asdad")
    val rdd = sc.parallelize(data)
    val r1 = rdd.flatMap(_.split(" ")).map(x => (x,1)).reduceByKey(_+_)
    // 重写排序规则
    implicit val myOrdering = new Ordering[String] {
      override def compare(x: String, y: String): Int = {
        x.length - y.length
      }
    }
    // 自定义分区器
    class MyPartitioner(partitions:Int) extends Partitioner {
      override def numPartitions: Int = partitions

      override def getPartition(key: Any): Int = {
        val k = key.asInstanceOf[String]
        Math.abs(k.hashCode % partitions)
      }
    }
    val r2 = r1.repartitionAndSortWithinPartitions(new MyPartitioner(5))
    println("glom=======")
    r2.glom().collect().foreach(x => {
      x.foreach(print)
      println()
    })
    sc.stop()
  }
}
