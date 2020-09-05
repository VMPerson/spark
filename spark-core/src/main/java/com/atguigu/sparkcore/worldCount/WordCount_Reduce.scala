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
object WordCount_Reduce {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("WordCount_Reduce"))
    val fileRDD: RDD[String] = sc.textFile("datas/1.txt")

    val rdd: RDD[String] = fileRDD.flatMap(_.split(" "))
    val mapRDD: RDD[(String, Int)] = rdd.map((_, 1))

    val kvRDD: RDD[Map[String, Int]] = mapRDD.map(kv => {
      Map[String, Int](kv)
    })


    val res: Map[String, Int] = kvRDD.reduce((map1, map2) => {
      map1.foldLeft(map2)((map, kv) => {
        val k = kv._1
        val v = kv._2
        val newValue = map.getOrElse(k, 0) + v
        map.updated(k, newValue)
      })
    })
    res.foreach(println)

    sc.stop


  }


}
