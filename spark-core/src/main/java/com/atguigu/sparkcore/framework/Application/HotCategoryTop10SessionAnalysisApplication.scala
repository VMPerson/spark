package com.atguigu.sparkcore.framework.Application

import com.atguigu.sparkcore.framework.Common.TApplication
import com.atguigu.sparkcore.framework.Controller.HotCategoryTop10SessionAnalysisController

/**
 * @ClassName: HotCategoryTop10Session
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/10  22:41
 * @Version: 1.0
 */
object HotCategoryTop10SessionAnalysisApplication extends App with TApplication {


  runApplication() {
    val controller: HotCategoryTop10SessionAnalysisController = new HotCategoryTop10SessionAnalysisController()
    controller.dispatch()
  }
}
