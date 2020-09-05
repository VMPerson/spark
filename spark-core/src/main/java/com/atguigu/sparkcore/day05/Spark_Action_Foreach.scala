package com.atguigu.sparkcore.day05

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark_Action_Foreach
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/6  23:57
 * @Version: 1.0
 */
object Spark_Action_Foreach {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDCreate")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4))


    //这种方式是将数据先从Exector 收集到Driver端，再进行遍历打印
    rdd.collect().foreach(println)
    println("================================================")
    //这种方式是分布式打印，在各自的Exector上执行各自的打印

    rdd.foreach(elem => {
      println("元素值为： " + elem + "当前线程名为： " + Thread.currentThread().getName)
    })
    //算子外部的代码是在Driver端上执行的
    //算子内部的代码都是在Exector上执行的
  }


}
