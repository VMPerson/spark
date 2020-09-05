package com.atguigu.sparkcore.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//获取第二个数据分区的数据
object Spark_Operator_MapPartition_Detil {
  def main(args: Array[String]): Unit = {

    val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("Spark_Operator_Map"))

    val listRdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8), 3)

    /* //获取2号分区的数据
     val res: RDD[Int] = listRdd.mapPartitionsWithIndex((index, data) => {
       if (index == 2) {
         data
       } else {
         Nil.iterator
       }
     })
     res.collect().foreach(println)
     sc.stop()*/

    /*
        //将数据和分区号绑定在一起进行打印
        val res: RDD[(Int, Int)] = listRdd.mapPartitionsWithIndex((index, data) => {
          data.map(i => {
            (index, i)
          })
        })
        res.collect().foreach(println)*/

    //只对一号分区得数据进行处理
    val res: RDD[Int] = listRdd.mapPartitionsWithIndex((index, data) => {
      if (index == 1) {
        data.map(_ * 2)
      } else {
        data
      }
    })
    res.collect().foreach(println)


    sc.stop()

  }


}
