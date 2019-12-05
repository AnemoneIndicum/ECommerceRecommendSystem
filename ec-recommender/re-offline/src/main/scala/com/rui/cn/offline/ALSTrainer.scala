package com.rui.cn.offline

import breeze.numerics.sqrt
import com.rui.cn.offline.UserRecommend.MONGODB_RATING_COLLECTION
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
/**
 * als参数设置
 *
 * @author zhangrl
 * @time 2019/11/25-11:14
 **/
object ALSTrainer {

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
    val ratingRDD: RDD[Rating] = OperateMongoDb.loadInfoAsDataFrame(sparkSession, MONGODB_RATING_COLLECTION).as[ProductRating].rdd.map(itme => {
      Rating(itme.userId, itme.productId, itme.score)
    }).cache()

    // 切分数据集 分为测试集和训练集
    val splits: Array[RDD[Rating]] = ratingRDD.randomSplit(Array(0.8, 0.2))
    val trainingRDD = splits(0)
    val testingRDD = splits(1)

    // 核心实现：输出最优参数
    adjustALSParams( trainingRDD, testingRDD )
    sparkSession.stop()

  }

  def adjustALSParams(trainData: RDD[Rating], testData: RDD[Rating]): Unit ={
    // 遍历数组中定义的参数取值
    val result = for( rank <- Array(5, 10, 20, 50); lambda <- Array(1, 0.1, 0.01) )
      yield {
        val model = ALS.train(trainData, rank, 10, lambda)
        val rmse = getRMSE( model, testData )
        ( rank, lambda, rmse )
      }
    // 按照rmse排序并输出最优参数
    println(result.minBy(_._3))
  }

  def getRMSE(model: MatrixFactorizationModel, data: RDD[Rating]): Double = {
    // 构建userProducts，得到预测评分矩阵
    val userProducts: RDD[(Int, Int)] = data.map(item=> (item.user, item.product) )
    val predictRating: RDD[Rating] = model.predict(userProducts)

    // 按照公式计算rmse，首先把预测评分和实际评分表按照(userId, productId)做一个连接
    val observed: RDD[((Int, Int), Double)] = data.map(item=> ( (item.user, item.product),  item.rating ) )
    val predict: RDD[((Int, Int), Double)] = predictRating.map(item=> ( (item.user, item.product),  item.rating ) )

    // 求均方根误差
    sqrt(
      observed.join(predict).map{
        case ( (userId, productId), (actual, pre) ) =>
          val err = actual - pre
          err * err
      }.mean()
    )
  }
}
