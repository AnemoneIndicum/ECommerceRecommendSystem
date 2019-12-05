package com.rui.cn


import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * todo
  *
  * @author zhangrl
  * @time 2019/11/21-13:26
  **/
object Statistics {


  /**
    * 原始数据 商品 评分 表明
    */
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_PRODUCT_COLLECTION = "Product"

  /**
    * 商品历史评分次数标
    */
  val RATE_MORE_PRODUCTS = "RateMoreProducts"
  /**
    * 近期的热门商品
    */
  val RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts"

  /**
    * 优质商品评分
    */
  val AVERAGE_PRODUCTS = "AverageProducts"

  def main(args: Array[String]): Unit = {

    val config: Map[String, String] = Map(
      "spark.cores" -> "local[4]",
      "mongo.url" -> "mongodb://hdp-00:27017/product",
      "mongo.db" -> "product"
    )

    // 创建sparkConfig
    val sparkConf: SparkConf = new SparkConf()
      .setMaster(config("spark.cores"))
      .setAppName(this.getClass.getSimpleName)

    // 创建sparkSesion
    val sparkSession: SparkSession = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    import sparkSession.implicits._
    // 创建mongodb配置对象
    implicit val mongoConfig: MongoConfig = MongoConfig(config("mongo.url"), config("mongo.db"))

    // 加载商品评分数据
    val ratingDF: DataFrame = OperateMongoDb.loadInfoAsDataFrame(sparkSession, MONGODB_RATING_COLLECTION).as[Rating].toDF()

    // 注册一个临时表
    ratingDF.createOrReplaceTempView("rating")
    // TODO: 统计
    // 1. 历史热门商品，按照评分个数统计，productId，count
    val historyProductRatingCountDF: DataFrame = sparkSession.sql("select productId, count(productId) as count from rating group by productId order by count desc")
    //    historyProductRatingCountDF.show()
    // 保存数据
    OperateMongoDb.dataFrameToSave(historyProductRatingCountDF, RATE_MORE_PRODUCTS)

    /** *****************************************************************************************/
    // 2. 近期热门商品，把时间戳转换成yyyyMM格式进行评分个数统计，最终得到productId, count, yearmonth
    // 创建一个日期格式化工具
    val simpleDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyyyMM")
    // 注册udf函数
    sparkSession.udf.register("changeDate", (x: Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)
    // 转换数据格式
    val ratingOfYearMonthDF = sparkSession.sql("select productId, score, changeDate(timestamp) as yearmonth from rating")
    ratingOfYearMonthDF.createOrReplaceTempView("ratingOfMonth")
    val rateMoreRecentlyProductsDF = sparkSession.sql("select productId, count(productId) as count, yearmonth from ratingOfMonth group by yearmonth, productId order by yearmonth desc, count desc")
    // 把df保存到mongodb
    OperateMongoDb.dataFrameToSave(rateMoreRecentlyProductsDF, RATE_MORE_RECENTLY_PRODUCTS)

    /** *****************************************************************************************/
    // 3. 优质商品统计，商品的平均评分，productId，avg
    val productRatingOfAvgDF: DataFrame = sparkSession.sql("select  productId, avg(score) as avg  from rating group by productId order by avg desc")
    OperateMongoDb.dataFrameToSave(productRatingOfAvgDF, AVERAGE_PRODUCTS)
    sparkSession.stop()
  }


}
