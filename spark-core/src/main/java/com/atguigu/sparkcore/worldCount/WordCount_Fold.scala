package com.atguigu.sparkcore.worldCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: WorldCount_Fold
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/7  8:49
 * @Version: 1.0
 */
object WordCount_Fold {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("WorldCount_Fold"))
    val fileRDD: RDD[String] = sc.textFile("datas/1.txt")

    val rdd: RDD[String] = fileRDD.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = rdd.map((_, 1))

    val kvRDD: RDD[Map[String, Int]] = mapRDD.map(kv => {
      Map[String, Int](kv)
    })
    val res: Map[String, Int] = kvRDD.fold(Map[String, Int]())((map1, map2) => {
      //归并两个map
      map1.foldLeft(map2)((map, kv2) => {
        var k = kv2._1
        var v = kv2._2
        val newValue = map.getOrElse(k, 0) + v
        map.updated(k, newValue)
      })
    })
    res.foreach(println)

    sc.stop


  }


}
