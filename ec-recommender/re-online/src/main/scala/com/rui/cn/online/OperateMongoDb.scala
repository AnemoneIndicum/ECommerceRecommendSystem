package com.rui.cn.online

import com.mongodb.casbah.commons.MongoDBObject
import com.rui.cn.online.Commons.MONGODB_STREAM_RECS_COLLECTION
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


  /**
    * 将数据保存到MongoDB    userId -> 1,  recs -> 22:4.5|45:3.8
    *
    * @param streamRecs  流式的推荐结果
    * @param mongoConfig MongoDB的配置
    */
  def saveRecsToMongoDB(userId: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit = {
    //到StreamRecs的连接
    val streamRecsCollection = RedisUtil.mongoClient(mongoConfig.dbName)(MONGODB_STREAM_RECS_COLLECTION)

    streamRecsCollection.findAndRemove(MongoDBObject("userId" -> userId))
    streamRecsCollection.insert(MongoDBObject("userId" -> userId,
      "recs" -> streamRecs.map(x => MongoDBObject("productId" -> x._1, "score" -> x._2))))

  }
}
