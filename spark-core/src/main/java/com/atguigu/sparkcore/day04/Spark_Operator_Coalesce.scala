package com.atguigu.sparkcore.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark_Operator_Coalesce {

  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Operator_Coalesce"))

    val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6), 3)

    // 根据数量减小分区，在大数据过滤后，对小数据集合进行减小分区，以提高执行效率
    // 方法有两个参数:
    //    第一个参数：分区数
    //    第二个参数：是否启动shuffle  默认不写不启用

    //  如果减小分区，第二个参数就不用写，如果第二个参数为true,则会将所有数据打乱重新分区
    //  如果要增大分区数，第二个参数必须为true,需要启动shuffle,
    //  若有扩大分区的需求，我们使用rePartition算子来实现
    //

    val res: RDD[Int] = rdd.coalesce(2,false)

    res.saveAsTextFile("output2")

    sc.stop()


  }


}
