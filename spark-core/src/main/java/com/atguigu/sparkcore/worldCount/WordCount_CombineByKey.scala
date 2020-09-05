package com.atguigu.sparkcore.worldCount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: WorldCount_CombineByKey
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/6  18:16
 * @Version: 1.0
 */
object WordCount_CombineByKey {

  def main(args: Array[String]): Unit = {


    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("WordCount_CombineByKey"))
    val fileRDD: RDD[String] = sc.textFile("datas/1.txt", 2)
    val rdd: RDD[(String, Int)] = fileRDD.flatMap(_.split(" ")).map((_, 1))

    val res: RDD[(String, Int)] = rdd.combineByKey(
      x => x,
      (x: Int, y: Int) => (x + y), //同去内key相同的两个value相加
      (m: Int, n: Int) => (m + n) //区间相同key的两个value相加
    )
    res.collect().foreach(println)
    sc.stop()

  }


}
