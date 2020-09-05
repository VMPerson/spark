package com.atguigu.sparkSql.day02

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @ClassName: Spark_Load_Temporary
 * @Description: TODO 当区区临时表的数据时，我们可以采用文件和表结合的方式来操作
 * @Author: VmPerson
 * @Date: 2020/8/13  21:27
 * @Version: 1.0
 */
object Spark_Load_Temporary {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Load_Temporary")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //读取临时文件时，采用文件和表格结合的方式来读取
    // 路径用`` 括起来
    spark.sql("select * from json.`datas/user.json`").show()

    println("=======================================================")
    spark.sql("select * from parquet.`datas/users.parquet`").show()

    spark.stop()


  }


}
