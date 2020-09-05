package com.atguigu.sparkcore.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountDemo2 {


  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("WordCount")
    var sc: SparkContext = new SparkContext(conf)

    var file: RDD[String] = sc.textFile("datas")

    var flatmap: RDD[String] = file.flatMap(_.split(" "))

    /*   var wordToOne = flatmap.map {
         case words => (words, 1)
       }*/
    //简化
    var wordToOne = flatmap.map((_, 1))

    /*    var res = wordToOne.reduceByKey {
          case (x: Int, y: Int) => {
            x + y
          }
        }*/
    //简化
    var res = wordToOne.reduceByKey(_ + _)

    res.collect().foreach(println)

  }


}
