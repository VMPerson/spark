package com.atguigu.sparkSql.day02

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @ClassName: Saprk_Read_Write
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/13  22:10
 * @Version: 1.0
 */
object Saprk_Read_Write {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Saprk_Read_Write")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //读取本地文件加载到数据库

    val df: DataFrame = spark.read.json("datas/user.json")
    df.write.format("jdbc")
      .option("url", "jdbc:mysql://hadoop102:3306/spark-sql")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("user", "root")
      .option("password", "123456")
      .option("dbtable", "user").mode(SaveMode.Append).save()
    spark.stop()


  }


}


