package com.atguigu.sparkcore.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_MkRDD1 {

  def main(args: Array[String]): Unit = {


    // TODO 从内存中创建RDD，进行数据的计算
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDDPartition")
    val sc = new SparkContext(sparkConf)

    // TODO 操作数据
    val list = List(1, 2, 3, 4, 5)

    //获取分区个数的方法  源码解读
    /*    def positions(length: Long, numSlices: Int): Iterator[(Int, Int)] = {
          (0 until numSlices).iterator.map { i =>
            val start = ((i * length) / numSlices).toInt
            val end = (((i + 1) * length) / numSlices).toInt
            (start, end)
          }
          positions(array.length, numSlices).map { case (start, end) =>
            array.slice(start, end).toSeq
          }.toSeq

          slice方法时from until
        */
    //0号分区    start: 0  end:2  所以包含的是【0,1】号索引位置的元素1,2
    //1号分区    start: 2  end:5  所以包含的是【2,3,4】号索引位置的元素 3,4,5

    //当我们指定分区数为2时
    /*    val res: RDD[Int] = sc.makeRDD(list, 2)
        res.saveAsTextFile("output2")
        sc.stop()*/


    //当我们指定分区为3时
    //分析流程
    // 0 =》 start:0  end:1  所以只包含0号索引位置的元素 1
    // 1 =》 start:1  end:3  所以包含【1，2】号索引位置元素 2，3
    // 2 =》 start:3  end:5  所以只包含【3，4】 号索引位置的元素 4，5
    val res: RDD[Int] = sc.makeRDD(list, 3)
    res.saveAsTextFile("output3")
    sc.stop()


  }


}
