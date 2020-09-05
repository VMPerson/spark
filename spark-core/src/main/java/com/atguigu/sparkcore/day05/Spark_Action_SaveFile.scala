package com.atguigu.sparkcore.day05

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: Spark_Action_SaveFile
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/6  23:32
 * @Version: 1.0
 */
object Spark_Action_SaveFile {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDCreate")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 2, 3, 4), 2)


    //以文本文件形式保存
    rdd.saveAsTextFile("output1")
    //读取
    //sc.textFile()

    //以对象形式保存
    rdd.saveAsObjectFile("output2")
    //读取
    //sc.objectFile()

    //以序列化文件形式保存
    rdd.map((_, 1)).saveAsSequenceFile("output3")
    //读取
    //sc.sequenceFile()

    sc.stop()


  }


}
