package com.atguigu.sparkcore.day02

//当对象需要序列化时，我们可以实现extends  Serializable 这个特质来完成、
//但实际开发过程中， 我们通过case 模式匹配样例类实现 case默认实现了序列化接口

case class Task() {

  val list: List[Int] = List(1, 2, 3, 4, 5)
  var logic: (Int, Int) => Int = (_ + _)


  def compute(): Int = {
    list.reduce(logic)

  }
}

object Task {

  def main(args: Array[String]): Unit = {

    println((new Task()).compute())



    /* val list = List(1,2,3,4)

     // 将数据两两结合，实现运算规则
     val i: Int = list.reduce( (x,y) => x-y )
     println("i = " + i)


 */


  }


}