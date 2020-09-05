package com.atguigu.sparkcore.worldCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: WorldCount_Aggregate
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/12  19:43
 * @Version: 1.0
 */
object WordCount_Aggregate {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("WordCount_Aggregate"))

    val fileRdd: RDD[String] = sc.textFile("datas/1.txt")

    val rdd: RDD[String] = fileRdd.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Int)] = rdd.map((_, 1))


    val res: Map[String, Int] = mapRDD.aggregate(Map[String, Int]())((map1, kv) => {
      val k: String = kv._1
      val v: Int = kv._2
      val newValue = map1.getOrElse(k, 0) + v
      map1.updated(k, newValue)
    }, ((map1, map2) => {
      map1.foldLeft(map2)((map, kv) => {
        val k: String = kv._1
        val v: Int = kv._2
        val newValue = map.getOrElse(k, 0) + v
        map.updated(k, newValue)
      })
    }))
    res.foreach(println)

  }

}
