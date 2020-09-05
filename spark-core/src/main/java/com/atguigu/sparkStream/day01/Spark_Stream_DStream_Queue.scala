package com.atguigu.sparkStream.day01

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}

import scala.collection.mutable



/**
 * @ClassName: Spark_Stream_DStream
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/14  21:20
 * @Version: 1.0
 */
object Spark_Stream_DStream_Queue {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Stream_DStream")
    //创建流对象
    val ssc: StreamingContext = new StreamingContext(sparkConf, Duration(3 * 1000))

    val queue: mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]()
    val inputStream: InputDStream[Int] = ssc.queueStream(queue)

    val res: DStream[Int] = inputStream.reduce(_ + _)
    res.print()

    ssc.start()

    while (true) {
      queue += ssc.sparkContext.makeRDD(1 to 10)
      Thread.sleep(3000)
    }

    ssc.awaitTermination()

  }


}
