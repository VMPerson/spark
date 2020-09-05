package com.atguigu.sparkStream.day02

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

/**
 * @ClassName: Spark_Stream_Exit
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/16  22:31
 * @Version: 1.0
 */
object Spark_Stream_Exit {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Stream_Exit")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))

    ssc.checkpoint("cp")
    val inputStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9988)


    val resDs: DStream[(String, Int)] = inputStream.map((_, 1)).updateStateByKey((valueSeq: Seq[Int], buffer: Option[Int]) => {
      //类似于reduce
      val newCount: Int = buffer.getOrElse(0) + valueSeq.sum
      //要求返回一个Option
      Option(newCount)
    })
    resDs.print()


    ssc.start()

    //业务升级 技术升级等，需要关闭服务才能
    //我们如何实现优雅关闭？？
    //在采集线程和main线程之间

    new Thread(new Runnable {

      override def run(): Unit = {

        //可以借助第三方存储工具
        //Mysql hadoop zk 等
        while (true) {
          val state: StreamingContextState = ssc.getState()
          if (state == StreamingContextState.ACTIVE) {
            //优雅关闭
            ssc.stop(true, true)
          }


        }


      }
    })


    ssc.awaitTermination()

  }


}
