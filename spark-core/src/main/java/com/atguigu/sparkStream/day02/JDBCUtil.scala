package com.atguigu.sparkStream.day02

import java.sql.{Connection, PreparedStatement}
import java.util.Properties

import com.alibaba.druid.pool.DruidDataSourceFactory
import javax.sql.DataSource

/**
 * @ClassName: JDBCUtil
 * @Description: TODO
 * @Author: VmPerson
 * @Date: 2020/8/18  21:19
 * @Version: 1.0
 */
object JDBCUtil {
    //初始化连接池
    var dataSource: DataSource = init()

    //初始化连接池方法
    def init(): DataSource = {
        val properties = new Properties()
        properties.setProperty("driverClassName", "com.mysql.jdbc.Driver")
        properties.setProperty("url", "jdbc:mysql://hadoop102:3306/Spark")
        properties.setProperty("username", "root")
        properties.setProperty("password", "123456")
        properties.setProperty("maxActive", "30")
        DruidDataSourceFactory.createDataSource(properties)
    }

    //获取MySQL连接
    def getConnection: Connection = {
        dataSource.getConnection
    }

    //执行SQL语句,单条数据插入
    def executeUpdate(connection: Connection, sql: String, params: Array[Any]): Int = {
        var rtn = 0
        var pstmt: PreparedStatement = null
        try {
            connection.setAutoCommit(false)
            pstmt = connection.prepareStatement(sql)

            if (params != null && params.length > 0) {
                for (i <- params.indices) {
                    pstmt.setObject(i + 1, params(i))
                }
            }
            rtn = pstmt.executeUpdate()
            connection.commit()
            pstmt.close()
        } catch {
            case e: Exception => e.printStackTrace()
        }
        rtn
    }
}
