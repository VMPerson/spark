package com.atguigu.sparkStream.day01

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
 * @ClassName: Spark_Stream_DStream
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/14  21:20
 * @Version: 1.0
 */
object Spark_Stream_DStream_File {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Stream_DStream_File")
    //创建流对象
    val ssc: StreamingContext = new StreamingContext(sparkConf, Duration(3 * 1000))

   val ds: DStream[String] = ssc.textFileStream("in")


    ds.print()

    ssc.start()

    ssc.awaitTermination()

  }


}
