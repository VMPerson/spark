package com.atguigu.sparkcore.day02

import java.io.{OutputStreamWriter, PrintWriter}
import java.net.Socket


object Client {

  def main(args: Array[String]): Unit = {

/*    val master = new Socket("localhost", 8888)

    println("已经和服务器建立连接......")*/


    //发送单个字节
    /*
          var stream: OutputStream = master.getOutputStream()
        stream.write('H')
        stream.flush()
        stream.close()*/

/*
    //发送字符串
    //流程分析  字节流--》转换流---》字符流

    val writer = new PrintWriter(
      new OutputStreamWriter(
        master.getOutputStream, "UTF-8"
      )
    )
    writer.write("CMD /c notepad")
    writer.flush()
    println("向服务器端发送指令 ： CMD /c notepad ")
    master.close()
*/


  }


}
