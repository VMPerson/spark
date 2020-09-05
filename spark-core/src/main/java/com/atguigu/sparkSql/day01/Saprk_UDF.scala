package com.atguigu.sparkSql.day01

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @ClassName: Saprk_UDF
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/11  21:29
 * @Version: 1.0
 */
object Saprk_UDF {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Saprk_UDF")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    val df: DataFrame = spark.read.json("datas/user.json")

    df.createOrReplaceTempView("user")

    spark.udf.register("prefix",(name:String)=>{
      "前缀"+name
    })

    val df2: DataFrame = spark.sql("select prefix(name) from user")
    df2.show()


    spark.stop()

  }


}
