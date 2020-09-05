package com.atguigu.sparkcore.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark_Show_CheckPoint
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/8  0:08
 * @Version: 1.0
 */
object Spark_Show_CheckPoint {

  def main(args: Array[String]): Unit = {


    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Show_CheckPoint"))

    sc.setCheckpointDir("cp")

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3))

    val firstMap: RDD[Int] = rdd.map(elem => {
      println("firstMap=====================")
      elem + 1
    })

    val mapRDD: RDD[(Int, Int)] = firstMap.map(elem => {
      (elem, 1)
    })
    mapRDD.checkpoint()
    println(mapRDD.toDebugString)
    mapRDD.collect().foreach(print)
    println(mapRDD.toDebugString)
    mapRDD.collect().foreach(print)
    sc.stop()

  }


}
