package com.atguigu.sparkStream.day02

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @ClassName: Spark_Stream_TransForm
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/16  19:42
 * @Version: 1.0
 */
object Spark_Stream_Join {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Stream_Join")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(10))
    val inputStream1: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9988)
    val inputStream2: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 8888)

    val ds1: DStream[(String, Int)] = inputStream1.flatMap(_.split(" ")).map((_, 1))
    val ds2: DStream[(String, Int)] = inputStream2.flatMap(_.split(" ")).map((_, 1))


    //使用join需要注意的点是 采集周期必须相同



    val resDs: DStream[(String, (Int, Int))] = ds1.join(ds2)
    resDs.print()

    ssc.start()
    ssc.awaitTermination()

  }


}
