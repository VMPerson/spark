package com.atguigu.sparkcore.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RDD_MkRDD {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("RDDPartition")
    val sc = new SparkContext(sparkConf)


    val list = List(1, 2, 3, 4)


    val readList: RDD[Int] = sc.makeRDD(list)
    //makeRDD函数有两个参数 （数据源, 分片个数）
    //将数据源的数据进行均匀切分
    //    当我们默认不传时：两种模式的默认分片机制不同
    //    本地模式local:   scheduler.conf.getInt("spark.default.parallelism", totalCores)
    //           totalCores：本机的CPU核数
    //                        local：不写就是1
    //                        local[4]:表示4核
    //                        local[*]:表示最大核数
    //    yarn模式：我们的虚拟机集群的总核数
    //           conf.getInt("spark.default.parallelism", math.max(totalCoreCount.get(), 2))

    readList.saveAsTextFile("output")


    //关闭
    sc.stop()


  }


}
