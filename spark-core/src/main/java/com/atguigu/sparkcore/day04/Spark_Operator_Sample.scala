package com.atguigu.sparkcore.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Operator_Sample {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Operator_Sample"))
    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 2)

    //sample算子即数据抽样

    //三个参数：
    //第一个参数：是否放回  true:表示放回抽取
    //                   false:表示不放回抽取
    //第二个参数：  当第一个参数为true时，第二个参数表示期望每个数据出现的次数
    //             当第一个参数为false时，第二个参数表示每条数据出现的概率，是每一条数据出现的概率，不是抽取总个数的概率
    //当三个参数：  相当于是一个随机种子，类似于打分，当这个随机种子不变时，那么数据的分数是不会改变的，随机数不随机，是通过随机算法来实现的

    val res: RDD[Int] = rdd.sample(false, 0.5)
    res.collect().foreach(println)

    sc.stop()

  }


}
