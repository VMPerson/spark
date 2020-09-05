package com.atguigu.sparkcore.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_MkFile1 {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD_MkFile1")
    val sc: SparkContext = new SparkContext(sparkConf)


    val fileRdd: RDD[String] = sc.textFile("datas/4.txt", 3)
    //此时文件大小为18k  我们设置的分片个数为3
    // 18/3  = 6 每个分区得字节数
    // 18/6  = 3 个分区
    // 0 号分区： 0 -6 但是是按行读取的所以0号分区的数据为 hello World
    // 1 号分区： 6 -12 但是6-12 数据已经被0号分区读取，所以一号分区数据为空
    // 2 号分区： 12 -18 所以2号分区的结果为Scala
    val result: Unit = fileRdd.saveAsTextFile("output2")

    sc.stop()


  }


}
