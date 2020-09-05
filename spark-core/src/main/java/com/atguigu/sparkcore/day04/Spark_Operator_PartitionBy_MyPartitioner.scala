package com.atguigu.sparkcore.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}


object Spark_Operator_PartitionBy_MyPartitioner {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Operator_PartitionBy_MyPartitioner"))

    //自定义分区器，继承Partitioner 实现两个方法 出入分区个数，根据我们的业务需求模式匹配返回分区号，从而达到分区的目的

    val rdd: RDD[(String, String)] = sc.makeRDD(List(("BMW", "X5"), ("BMW", "X7"), ("PORSCHE", "PALAMEILA"), ("PORSCHE", "KAYAN"), ("BENCHI", "C200")))
    val res: RDD[(String, String)] = rdd.partitionBy(new MyPartitioner(3))
    res.saveAsTextFile("output4")
    sc.stop()

  }

}

class MyPartitioner(num: Int) extends Partitioner {
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = {
    key match {
      case "BMW" => 0
      case "PORSCHE" => 1
      case "BENCHI" => 2
    }


  }
}
