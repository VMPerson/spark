package com.atguigu.sparkStream.day01

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
 * @ClassName: Spark_Stream_WordCount
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/14  20:52
 * @Version: 1.0
 */
object Spark_Stream_WordCount {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Stream_WordCount")
    //创建流对象
    val ssc: StreamingContext = new StreamingContext(sparkConf, Duration(3 * 1000))
    //开始发送数据
    val rs: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
    val ds: DStream[String] = rs.flatMap(_.split(" "))
    val map: DStream[(String, Int)] = ds.map((_, 1))
    val res: DStream[(String, Int)] = map.reduceByKey(_ + _)


    //SparkStreaming不能关闭，因为采集器要源源不断的采集数据
    //main方法执行完毕，Driver端程序也会结束，exector也就结束
    //start会启动新的线程进行数据采集，和Driver不是同一个线程
    //启动采集线程后，Driverd端是不能停止的，需要阻塞main线程，知道采集器停止

    res.print()
    ssc.start()
    ssc.awaitTermination()

  }


}
