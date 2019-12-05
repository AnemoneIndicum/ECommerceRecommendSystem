package com.rui.cn.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.jblas.DoubleMatrix

/**
  * 商品的相似度
  *
  * @author zhangrl
  * @time 2019/11/22-13:45
  **/
object ProductRecommend {
  // 定义mongodb中存储的表名
  val MONGODB_RATING_COLLECTION = "Rating"

  // 商品相似度矩阵
  val PRODUCT_RECS = "ProductRecs"

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
    // 定义隐式对象
    implicit val mongoConfig = MongoConfig(config("mongo.url"), config("mongo.db"))
    import sparkSession.implicits._
    // 加载数据userId: Int, productId: Int, score: Double,
    val ratingDf: DataFrame = OperateMongoDb.loadInfoAsDataFrame(sparkSession, MONGODB_RATING_COLLECTION)
      .select($"userId", $"productId", $"score").cache()
    // 创建训练集
    val trainData: RDD[Rating] = ratingDf.map(item => {
      Rating(item.getAs[Int]("userId"), item.getAs[Int]("productId"), item.getAs[Double]("score"))
    }).rdd
    // 定义模型训练的参数，rank隐特征个数，iterations迭代次数，lambda正则化系数
    val (rank, iterations, lambda) = (5, 10, 0.01)
    // 创建训练模型
    val model: MatrixFactorizationModel = ALS.train(trainData, rank, iterations, lambda)

    // 1. 利用商品的特征向量，计算商品的相似度列表
    val productFeatures: RDD[(Int, DoubleMatrix)] = model.productFeatures.map({
      case (productId, features) => (productId, new DoubleMatrix(features))
    })

    // 两两配对计算余弦相似度(自己跟自己迪卡集)
    val productResDF: DataFrame = productFeatures.cartesian(productFeatures)
      .filter { // 排除相同的商品
        case (a, b) => a._1 != b._1
      } // 计算余弦相似度
      .map {
      case (a, b) =>
        val simScore = cosineSimilarity(a._2, b._2)
        (a._1, (b._1, simScore))
    }
      .filter(_._2._2 > 0.4)
      .groupByKey()
      .map {
        case (productId, resc) =>
          RecommendationProduct(productId, resc.toList.sortWith(_._2 > _._2).map(x => Recommendation(x._1, x._2)))
      }
      .toDF()
    //    productResDF.show()
    // 保存数据库
    OperateMongoDb.dataFrameToSave(productResDF, PRODUCT_RECS)

    ratingDf.unpersist()
    sparkSession.stop()
  }

  /**
    * 余弦相似度
    *
    * @param vec1
    * @param vec2
    * @return
    */
  private def cosineSimilarity(vec1: DoubleMatrix, vec2: DoubleMatrix): Double = {
    vec1.dot(vec2) / (vec1.norm2() * vec2.norm2())
  }
}
