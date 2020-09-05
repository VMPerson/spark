package com.atguigu.sparkcore.worldCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: WorldCount_CountByKey
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/6  23:12
 * @Version: 1.0
 */
object WordCount_CountByKey {


  def main(args: Array[String]): Unit = {
    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("WordCount_CountByKey"))
    val fileRDD: RDD[String] = sc.textFile("datas/1.txt", 2)
    val rdd: RDD[(String, Int)] = fileRDD.flatMap(_.split(" ")).map((_, 1))

    val res: collection.Map[String, Long] = rdd.countByKey()
    res.foreach(println)


  }


}
