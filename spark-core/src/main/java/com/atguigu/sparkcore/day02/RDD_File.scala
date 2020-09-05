package com.atguigu.sparkcore.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_File {

  def main(args: Array[String]): Unit = {


    var sprkConf: SparkConf = new SparkConf().setMaster("local").setAppName("RDD_FIle")
    val sc: SparkContext = new SparkContext(sprkConf)

    //当读取本地文件时，可以有两种方式 据对路径和相对路径
    //所谓的绝对路径便是全路径
    //相对路径的话：Idea里面相对路径是以【项目】的【根路径】为基准的
    //yarn模式的话：  HDFS路径

    //TextFile读取文件时是一行一行去读取的，所以每一行其实就是字符串，处理数据的类型就是字符串，
    //spark读取文件，采用的是hadoop读取文件的方式
    //spark读取文件时，可以设定多个目录，一次性读取多个文件，也可采用通配符的方式读取文件
    val readFile: RDD[String] = sc.textFile("datas")

    readFile.collect().foreach(println)
    sc.stop()


  }


}
