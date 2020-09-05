package com.atguigu.sparkSql.day02

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 * @ClassName: Saprk_Save_Data
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/13  21:33
 * @Version: 1.0
 */
object Saprk_Save_Data {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Saprk_Save_Data")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    //读取数据时，我们采用的是sparkSession对象
    //保存数据时，我们需要使用DataFrame,DataSet
    val df: DataFrame = spark.read.json("datas/user.json")

    df.write.format("json").mode(SaveMode.Append).save("output")

    spark.stop()


  }


}
