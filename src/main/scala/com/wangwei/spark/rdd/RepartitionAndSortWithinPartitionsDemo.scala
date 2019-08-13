package com.wangwei.spark.rdd


import org.apache.spark.{Partitioner, SparkConf, SparkContext}

object RepartitionAndSortWithinPartitionsDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hello").setMaster("spark://192.168.1.113:7077")
    val sc = new SparkContext(conf)
    val rdd = sc.textFile("file:///root/spark_tmp/word.txt")
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
    r2.saveAsTextFile("file:///root/spark_tmp/2")
    sc.stop()
  }
}
