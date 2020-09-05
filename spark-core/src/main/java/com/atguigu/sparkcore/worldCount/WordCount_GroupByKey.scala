package com.atguigu.sparkcore.worldCount

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @ClassName: WorldCount_GroupByKey
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/6  17:59
 * @Version: 1.0
 */
object WordCount_GroupByKey {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("WordCount_GroupByKey"))

    val fileRDD: RDD[String] = sc.textFile("datas/1.txt")

    val rdd: RDD[String] = fileRDD.flatMap(_.split(" "))

    val mapRDD: RDD[(String, Int)] = rdd.map((_, 1))

    val groupRdd: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

    val res: RDD[(String, Int)] = groupRdd.map(elem => {
      (elem._1, elem._2.sum)
    })
    res.collect foreach println

    sc.stop()

  }








}
