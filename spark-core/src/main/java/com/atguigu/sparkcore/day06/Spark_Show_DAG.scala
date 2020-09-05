package com.atguigu.sparkcore.day06

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark_Show_DAG
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/7  22:17
 * @Version: 1.0
 */
object Spark_Show_DAG {

  def main(args: Array[String]): Unit = {


    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Show_DAG"))
    val fileRDD: RDD[String] = sc.textFile("datas/1.txt", 2)
    println(fileRDD.toDebugString)
    println("=====================================")
    val rdd: RDD[String] = fileRDD.flatMap(_.split(" "))
    println(rdd.toDebugString)
    println("=====================================")
    val mapRDD: RDD[(String, Int)] = rdd.map((_, 1))
    println(mapRDD.toDebugString)
    println("=====================================")
    val resRdd: collection.Map[String, Long] = mapRDD.countByKey()


    resRdd.foreach(println)
    sc.stop()


  }


}
