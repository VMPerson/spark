package com.atguigu.sparkStream.day01

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
 * @ClassName: Spark_Stream_DStream_Reveiver
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/14  21:53
 * @Version: 1.0
 */
object Spark_Stream_DStream_Reveiver {


  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Stream_DStream_Reveiver")
    //创建流对象
    val ssc: StreamingContext = new StreamingContext(sparkConf, Duration(3 * 1000))

    val ds: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver(1000))

    ds.print()

    ssc.start()
    ssc.awaitTermination()


  }


  class MyReceiver(sleepTime: Long) extends Receiver[String](StorageLevel.MEMORY_ONLY) {
    var flg: Boolean = true

    override def onStart(): Unit = {

      while (flg) {
        var str = "自定义数据源" + System.currentTimeMillis()
        store(str)
        Thread.sleep(sleepTime)

      }


    }

    override def onStop(): Unit = {
      flg = false
    }
  }


}
