package com.atguigu.sparkcore.day02

import java.io.ObjectOutputStream
import java.net.Socket

object Master {


  def main(args: Array[String]): Unit = {
    /*
        val serverSocket = new ServerSocket(8888)
        println("服务器端启动，等待连接.....")
        val client = serverSocket.accept()*/

    //字节流
    /*   val inputStream = client.getInputStream
       val i = inputStream.read()
       println("服务器接收到数据.....+" + i)*/

    /*    //字符流
        val reader = new BufferedReader(
          new InputStreamReader(
            client.getInputStream, "UTF-8"
          )
        )
        //获取客户端发送的指令
        val str = reader.readLine()
        println("客户端发送的指令为： " + str)
        //执行客户端发送的指令
        Runtime.getRuntime.exec(str)

        client.close()
        serverSocket.close()*/


    //向worker发送任务
    val socket = new Socket("localhost", 9999)
    println("连接到Worker节点.....")


    val stream = new ObjectOutputStream(
      socket.getOutputStream)
    stream.writeObject(new Task)
    stream.flush()
    socket.close()

  }


}
