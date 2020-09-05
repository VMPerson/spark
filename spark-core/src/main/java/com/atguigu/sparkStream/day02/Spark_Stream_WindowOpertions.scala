package com.atguigu.sparkStream.day02

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ClassName: Spark_Stream_WindowOpertions
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/16  19:42
 * @Version: 1.0
 */
object Spark_Stream_WindowOpertions {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Stream_WindowOpertions")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    ssc.checkpoint("cp")
    val inputStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9988)

    //传递两个参数  窗口的长度，滑动的步长   注意这两个参数都必须是采集周期的整数倍
    val ds: DStream[(String, Int)] = inputStream.map((_, 1)).window(Seconds(3), Seconds(6)).reduceByKey(_ + _)
    ds.print()

    //这个算子其实就是上述几个算子的综合体
    val resDs: DStream[(String, Int)] = inputStream.map((_, 1)).reduceByKeyAndWindow((x: Int, y: Int) => {
      x + y
    }, Seconds(3), Seconds(6))
    resDs.print()

    //如果滑动窗口的幅度较小，滑动窗口的范围较大，那么就会存在大量的重复数据
    //为了避免重复计算，可以采用特殊的方法
    //需要使用检查点的操作
    val resDs2: DStream[(String, Int)] = inputStream.map((_, 1)).reduceByKeyAndWindow(
      (x: Int, y: Int) => x + y, //新增的数据
      (a: Int, b: Int) => a - b //重复的数据
      , Seconds(3), Seconds(6))
    resDs2.print()


    ssc.start()
    ssc.awaitTermination()

  }


}
