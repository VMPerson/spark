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
object Spark_Stream_TransForm {

  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark_Stream_TransForm")
    val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))


    val inputStream: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)


    //这两种方式有何区别？
    // transForm:  是将RDD作为一个整体然后，rDD再做处理
    // 而map是将所有数据作为一个槽、整体处理
    //这样的话，我们transform可以在内部做其他操作，而map内部不能
    //下面我们对两种方式进行详细处理


    //这里是可以写代码的  Driver端执行 只会执行一次
    val ds: DStream[(String, Int)] = inputStream.transform(
      //这里不能写代码
      rdd => {
        //这里可以写代码  RDD外部 Driver端执行    每个采集周期执行1次：周期性执行  原因：RDD算子外执行的代码在Driver端，RDD算子内执行的代码在Executor端
        rdd.map(
          //这里不能写代码
          elem => {
            //这里可以写代码   RDD内部  Executor端执行
            (elem, 1)
          })
      }
    )

    //val ds2: DStream[(String, Int)] = inputStream.map((_,1))
    //这里可以写代码  这里是算子外部  Driver端执行
    inputStream.map(
      //这里不能写逻辑 匿名函数这里不能写代码（除非有控制抽象）
      elem => {
        //这里可以写代码 业务逻辑这里是可以写 这里其实就是算子内部 Executor端执行
        (elem, 1)
      })


  }


}
