package com.rui.cn

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * 创建数据库记录
  *
  * @author zhangrl
  * @time 2019/11/21-14:05
  **/
object OperateMongoDb {

  /**
    * 保存DataFrame数据集到mongodb
    *
    * @param df          DataFrame数据集
    * @param tabName     collection表名
    * @param mongoConfig 数据库配置
    */
  def dataFrameToSave(df: DataFrame, tabName: String)(implicit mongoConfig: MongoConfig): Unit = {
    df
      .write
      .option("uri", mongoConfig.url)
      .option("collection", tabName)
      .mode(SaveMode.Overwrite)
      .format("com.mongodb.spark.sql")
      .save()
  }

  /**
    * mongodb加载数据
    *
    * @param sparkSession SparkSession
    * @param tabName      collection表明
    * @param mongoConfig  数据库配置
    * @return
    */
  def loadInfoAsDataFrame(sparkSession: SparkSession, tabName: String)(implicit mongoConfig: MongoConfig): DataFrame = {
    sparkSession.read
      .option("uri", mongoConfig.url)
      .option("collection", tabName)
      .format("com.mongodb.spark.sql")
      .load()
  }
}
