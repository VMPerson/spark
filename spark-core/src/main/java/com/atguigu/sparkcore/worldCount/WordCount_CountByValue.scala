package com.atguigu.sparkcore.worldCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: WorldCount_CountByValue
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/6  23:28
 * @Version: 1.0
 */
object WordCount_CountByValue {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("WordCount_CountByValue"))
    val fileRDD: RDD[String] = sc.textFile("datas/1.txt", 2)
    val rdd: RDD[String] = fileRDD.flatMap(_.split(" "))

    val res: collection.Map[String, Long] = rdd.countByValue()
    res foreach println

    sc.stop()

  }


}
