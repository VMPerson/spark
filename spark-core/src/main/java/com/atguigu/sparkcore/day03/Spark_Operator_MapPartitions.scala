package com.atguigu.sparkcore.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//获取每个数据分区的最大值  使用mapPartitions算子
object Spark_Operator_MapPartitions {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Operator_Map"))

    val listRdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4),2)

    //使用mapPartitons算子进行运算
    //mapPartiton算子要求传入的是可迭代集合，返回的也是可迭代集合
    val res: RDD[Int] = listRdd.mapPartitions(datas => {
      List(datas.max).iterator
    })
    res.saveAsTextFile("output1")
    sc.stop()

  }


}
