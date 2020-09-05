package com.atguigu.sparkSql.day01

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @ClassName: DataFrameTest
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/11  15:20
 * @Version: 1.0
 */
object SparkSQL_DataFrame {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DataFrameTest")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import spark.implicits._

    //创建一个DataFrame
    val dataFrame: DataFrame = spark.read.json("datas/user.json")


    //sql风格语法
    dataFrame.createOrReplaceTempView("user")
    spark.sql("select name from user").show()
    println("======================================================")

    //DSL风格语法
    dataFrame.select("name", "age").show()


    //DSL风格的隐士转换
    println("==========================隐士转换===================")
    dataFrame.select($"name", $"age" + 1).show()
    println("===================================================================")
    dataFrame.select('name, 'age + 1).show()


    spark.stop()

  }


}
