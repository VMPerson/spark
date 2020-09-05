package com.atguigu.sparkcore.framework.Dao

import com.atguigu.sparkcore.framework.Utils.EnvUtil

/**
 * @ClassName: TDao
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/10  22:31
 * @Version: 1.0
 */
trait TDao {
  def readFile(path: String) = {
    EnvUtil.get().textFile(path)
  }

}
