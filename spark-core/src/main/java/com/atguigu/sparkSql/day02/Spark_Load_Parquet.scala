package com.atguigu.sparkSql.day02

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @ClassName: Spark_Load_Parquet
 * @Description: TODO spark默认加载数据的格式是parquet格式的数据，我们加载指定格式的数据地时候，我们需要修改加载方式
 * @Author: VmPerson
 * @Date: 2020/8/13  21:03
 * @Version: 1.0
 */
object Spark_Load_Parquet {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Load_Parquet")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    println("===============================load默认数据格式加载===============================")
    //spark默认加载数据的格式是parquet
    spark.read.load("datas/users.parquet").show()

    println("================================指定格式加载json数据===============================")
    //指定格式读取数据
    spark.read.format("json").load("datas/people.json").show()

    println("=========================指定格式加载orc数据=======================================")
    spark.read.format("orc").load("datas/users.orc").show()

    println("=========================指定格式加载csv数据=======================================")
    spark.read.format("csv").load("datas/people.csv").show()

    spark.stop()

  }


}
