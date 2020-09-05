package com.atguigu.sparkcore.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Operator_Map_Detil_2 {


  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Operator_Map_Detil_2"))


    val list: List[Int] = List(1, 2, 3, 4)
    val rdd: RDD[Int] = sc.makeRDD(list, 2)


    //我们先做一次map算子
    val rdd1: RDD[Int] = rdd.map((i: Int) => {
      println("num1   >>>>>>>>" + i)
      i * 2
    })

    //再进行一次map算子运算
    val rdd2: RDD[Int] = rdd1.map((i: Int) => {
      println("num2   ********" + i)
      i * 2
    })

    //运行结果为
    //num1   >>>>>>>>3
    //num2   ********6
    //num1   >>>>>>>>4
    //num2   ********8
    //num1   >>>>>>>>1
    //num2   ********2
    //num1   >>>>>>>>2
    //num2   ********4

    //案例分析：我们设置的两个分区 所以数据源（1,2）在0号分区  （3,4）在一号分区
    //0号分区和1号分区并行执行，每个分区内有序，就是在0号分区内1线执行当1所有执行完之后2再执行

    rdd2.collect
    sc.stop()
  }

}

