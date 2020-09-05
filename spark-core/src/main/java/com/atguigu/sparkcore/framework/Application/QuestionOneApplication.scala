package com.atguigu.sparkcore.framework.Application

import com.atguigu.sparkcore.framework.Common.TApplication
import com.atguigu.sparkcore.framework.Controller.QuestionOneController

/**
 * @ClassName: WorldCountApplication
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/10  9:49
 * @Version: 1.0
 */
object QuestionOneApplication extends App with TApplication {

  runApplication()({
    val controller: QuestionOneController = new QuestionOneController
    controller.execute()
  })

}
