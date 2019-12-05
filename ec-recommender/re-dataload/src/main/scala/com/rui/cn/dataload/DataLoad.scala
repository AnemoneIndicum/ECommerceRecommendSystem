package com.rui.cn.dataload

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 加载数据
  *
  * @author zhangrl
  * @time 2019/11/21-10:01
  **/
object DataLoad {

  /**
    * 定数数据路径
    */
  val PRODUCT_PATH = "G:\\project\\hadoop\\ECommerceRecommendSystem\\ec-recommender\\re-dataload\\src\\main\\resources\\products.csv"
  val RATING_PATH = "G:\\project\\hadoop\\ECommerceRecommendSystem\\ec-recommender\\re-dataload\\src\\main\\resources\\ratings.csv"

  def main(args: Array[String]): Unit = {
    val config: Map[String, String] = Map(
      "spark.cores" -> "local[*]",
      "mongo.url" -> "mongodb://hdp-00:27017/product",
      "mongo.db" -> "product"
    )

    val sparkConf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName).setMaster(config("spark.cores"))

    // sparkSession
    val sparkSession: SparkSession = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    import sparkSession.implicits._
    // 加载数据
    val productDF: DataFrame = sparkSession.sparkContext.textFile(PRODUCT_PATH).map(item => {
      val attr: Array[String] = item.split("\\^")
      Product(attr(0).toInt, attr(1).trim, attr(4).trim, attr(5).trim, attr(6).trim)
    }).toDF()

    val ratingDF: DataFrame = sparkSession.sparkContext.textFile(RATING_PATH).map(item => {
      val attr: Array[String] = item.split(",")
      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).toDF()

    // 缓存
    ratingDF.cache()
    productDF.cache()

    // 定义隐式对象
    implicit val mongoConfig = MongoConfig(config("mongo.url"), config("mongo.db"))

    // 写入数据库
    CreateDataRecord.storeDataInMongo(productDF,ratingDF)

    // 去除缓存
    ratingDF.unpersist()
    productDF.unpersist()
    sparkSession.stop()
  }

}
