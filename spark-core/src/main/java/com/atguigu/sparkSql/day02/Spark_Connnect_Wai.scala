package com.atguigu.sparkSql.day02

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @ClassName: Spark_Connnect_Wai
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/13  23:50
 * @Version: 1.0
 */
object Spark_Connnect_Wai {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Connnect_Wai")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    spark.sql("show tables").show()

    spark.stop()


  }


}
