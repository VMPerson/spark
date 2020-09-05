package com.atguigu.sparkcore.day02

import java.io.ObjectInputStream
import java.net.ServerSocket

object Worker {

  def main(args: Array[String]): Unit = {

    //开启服务器，接收master发送的任务
    val server = new ServerSocket(9999)
    println("服务已经开启，等待发送任务......")
    val socket = server.accept()

    //获取对象流
    val stream = new ObjectInputStream((socket.getInputStream))
    val task = stream.readObject().asInstanceOf[Task]

    //执行计算任务
    println("任务的执行结果为： " + task.compute())


  }

}
