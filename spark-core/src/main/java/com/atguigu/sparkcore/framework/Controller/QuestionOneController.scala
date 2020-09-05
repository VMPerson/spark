package com.atguigu.sparkcore.framework.Controller

import com.atguigu.sparkcore.framework.Service.QuestionOneService

/**
 * @ClassName: WorldCountController
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/10  9:54
 * @Version: 1.0
 */
class QuestionOneController {

  private val service: QuestionOneService = new QuestionOneService

  def execute() = {
    val res: Array[(String, (Int, Int, Int))] = service.execute()
    res.foreach(println)
  }

}
