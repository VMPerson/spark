package com.atguigu.sparkSql.day01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName: SparkSQL_RDD_DataFrame
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/11  21:06
 * @Version: 1.0
 */
object SparkSQL_RDD_DataFrame {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL_RDD_DataFrame")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    val sc: SparkContext = spark.sparkContext
    val rdd: RDD[(String, Int)] = sc.makeRDD(List(
      ("zhangsan", 20),
      ("lisi", 10),
      ("wangwu", 43),
      ("zhaoliu", 45)
    ))

    val mapRDD: RDD[User] = rdd.map(elem => {
      User(elem._1, elem._2)
    })

    //RDD转DataFrame
    val frame: DataFrame = rdd.toDF("username","age")
    frame.show()

    val dataFrame: DataFrame = spark.createDataFrame(mapRDD)
    dataFrame.show()

    //DataFrame转化为RDD
    val resultRDD: RDD[Row] = dataFrame.rdd
    resultRDD.foreach(println)


  }


}
