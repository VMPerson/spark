package com.atguigu.sparkcore.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_MkFile {

  def main(args: Array[String]): Unit = {


    // TODO 从文件中创建RDD，进行数据的计算
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDPartition")
    val sc = new SparkContext(sparkConf)


    //当我们指定参数2个分区
    // 文件大小totalSize=7
    // 7/2 =3   每个分区三个字节
    // 7/3 =2  但是超过 1.1倍数所以再加一个分区 即3个
    // 索引从0开始 0=》【0,3】  第一行三个字节 第二行第一个字节为B ,所以打印AB
    // 1=》【3,6】 打印C
    // 2=》【6,7】 空

    val res: RDD[String] = sc.textFile("datas/3.txt", 2)
    res.saveAsTextFile("output1")
    sc.stop()


  }


}
