package com.rui.cn.online

import java.io.InputStream
import java.util.Properties

/**
  * 获取property文件属性
  *
  * @author zhangrl
  * @time 2019/7/22-11:31
  **/
object PropertyUtil {

   val properties = new Properties()
  try {
    val inputStream: InputStream = ClassLoader.getSystemResourceAsStream("application.properties")
    properties.load(inputStream)
  } catch {
    case e => println(e)
  } finally {

  }

  /**
    * 获取属性值
    * @param Key
    * @return
    */
  def getProperty(Key: String) = properties.getProperty(Key)
}
