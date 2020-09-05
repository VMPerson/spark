package com.atguigu.sparkcore.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Operator_Glom_Detil {

  //将所有数据珍禾味一个整体进行处理
  def main(args: Array[String]): Unit = {


    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Operator_Glom_Detil"))

    val rdd = sc.makeRDD(
      List(1, 3, 5, 4, 2, 6), 3
    )
    // TODO 计算所有分区最大值求和（分区内取最大值，分区间最大值求和）
    val res: RDD[Array[Int]] = rdd.glom()
    //分区最大值
    val tis: RDD[Int] = res.map(arr => arr.max)
    //求和
    println(tis.collect().sum)
    sc.stop()

  }


}
