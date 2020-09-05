package com.atguigu.sparkSql.day02

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * @ClassName: Saprk_Read_MySQL
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/13  21:53
 * @Version: 1.0
 */
object Saprk_Read_MySQL {

  def main(args: Array[String]): Unit = {


    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Saprk_Read_MySQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()


    //从关系型数据库mysql里面加载数据
    spark.read.format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/spark-sql")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user")
      .load().show()

    spark.stop()

  }


}
