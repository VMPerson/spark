package com.atguigu.sparkcore.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountDemo {


  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc: SparkContext = new SparkContext(sparkConf)

    //读取数据
    var file: RDD[String] = sc.textFile("datas")
    //进行扁平化处理
    var fm: RDD[String] = file.flatMap(_.split(" "))
    //分组处理数据
    var groupdata: RDD[(String, Iterable[String])] = fm.groupBy(str => str)

    //进行map映射处理
    var res: RDD[(String, Int)] = groupdata.map(kv => (kv._1, kv._2.size))
    //遍历打印
    res.collect().foreach(println)

    //关闭资源
    sc.stop()


  }


}
