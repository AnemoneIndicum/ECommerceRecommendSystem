package com.rui.cn.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 用户推荐
  *
  * @author zhangrl
  * @time 2019/11/21-18:14
  **/
object UserRecommend {
  // 定义mongodb中存储的表名
  val MONGODB_RATING_COLLECTION = "Rating"
  val USER_MAX_RECOMMENDATION = 20

  /**
    * 用户推荐、商品推荐
    */
  val USER_RECS = "UserRecs"
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
    // 加载数据
    val ratingRDD: RDD[(Int, Int, Double)] = OperateMongoDb.loadInfoAsDataFrame(sparkSession, MONGODB_RATING_COLLECTION).as[ProductRating].rdd.map(itme => {
      (itme.userId, itme.productId, itme.score)
    }).cache()

    // 提取所有用户和商品的数据集
    val userRDD: RDD[Int] = ratingRDD.map(_._1).distinct()
    val productRDD: RDD[Int] = ratingRDD.map(_._2).distinct()

    // todo 核心计算过程
    // 1. 训练隐语义模型
    val trainData: RDD[Rating] = ratingRDD.map(x => Rating(x._1, x._2, x._3))
    // 定义模型训练的参数，rank隐特征个数，iterations迭代次数，lambda正则化系数
    val (rank, iterations, lambda) = (5, 10, 0.01)

    // 创建训练模型
    val model: MatrixFactorizationModel = ALS.train(trainData, rank, iterations, lambda)
    /********************************************************************************************************/

    // 2. 获得预测评分矩阵，得到用户的推荐列表 RDD[(Int, Int)]
    // 用userRDD和productRDD做一个笛卡尔积，得到空的userProductsRDD表示的评分矩阵
    val userProducts: RDD[(Int, Int)] = userRDD.cartesian(productRDD)
    // 预测结果
    val preRating: RDD[Rating] = model.predict(userProducts)
    // 从预测评分矩阵中获取推荐列表
    // 获取用户的推荐结果
    val userRecs: DataFrame = preRating
      .filter(_.rating > 0)
      .map(rating => (rating.user, (rating.product, rating.rating)))
      .groupByKey()
      .map {
        case (uid, recs) => RecommendationUser(uid, recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION).map(x => Recommendation(x._1, x._2)))
      }.toDF()
    //    userRecs.show()
    // 保存数据
    OperateMongoDb.dataFrameToSave(userRecs, USER_RECS)
    ratingRDD.unpersist()

    /********************************************************************************************************/
    sparkSession.stop()

  }

}
