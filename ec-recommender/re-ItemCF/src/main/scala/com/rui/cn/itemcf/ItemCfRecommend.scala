package com.rui.cn.itemcf

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 基于物品的协同过滤
  *
  * @author zhangrl
  * @time 2019/11/26-19:34
  **/
object ItemCfRecommend {
  // 定义常量和表名
  val MONGODB_RATING_COLLECTION = "Rating"
  val ITEM_CF_PRODUCT_RECS = "ItemCFProductRecs"
  val MAX_RECOMMENDATION = 10

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
    // 定义隐式对象
    implicit val mongoConfig = MongoConfig(config("mongo.url"), config("mongo.db"))

    // 加载数据，转换成DF进行处理
    val ratingDF = OperateMongoDb.loadInfoAsDataFrame(sparkSession, MONGODB_RATING_COLLECTION)
      .as[Rating]
      .map(
        x => (x.userId, x.productId, x.score)
      )
      .toDF("userId", "productId", "score")
      .cache()

    // TODO: 核心算法，计算同现相似度，得到商品的相似列表
    // 统计每个商品的评分个数，按照productId来做group by
    val productRatingCountDF: DataFrame = ratingDF.groupBy("productId").count()
    // 在原有的评分表上rating添加count
    val ratingOfCountDF: DataFrame = ratingDF.join(productRatingCountDF, "productId")

    // 将评分按照用户id两两配对，统计两个商品被同一个用户评分过的次数
    val joinDF: DataFrame = ratingOfCountDF.join(ratingOfCountDF, "userId")
      .toDF("userId", "product1", "score1", "count1", "product2", "score2", "count2")
      .select("userId", "product1", "count1", "product2", "count2")
    // 创建一张临时表，用于写sql查询
    joinDF.createOrReplaceTempView("joined")

    // 按照product1,product2 做group by，统计userId的数量，就是对两个商品同时评分的人数
    val cooccurrenceDF: DataFrame = sparkSession.sql(
      """
        |select product1
        |, product2
        |, count(userId) as cocount
        |, first(count1) as count1
        |, first(count2) as count2
        |from joined
        |group by product1, product2
      """.stripMargin
    ).cache()

    // 提取需要的数据，包装成( productId1, (productId2, score) )
    val simDF: DataFrame = cooccurrenceDF.map {
      row =>
        val coocSim = cooccurrenceSim(row.getAs[Long]("cocount"), row.getAs[Long]("count1"), row.getAs[Long]("count2"))
        (row.getInt(0), (row.getInt(1), coocSim))
    }
      .rdd
      .groupByKey()
      .map {
        case (productId, recs) =>
          RecommendationProduct(productId, recs.toList
            .filter(x => x._1 != productId)
            .sortWith(_._2 > _._2)
            .take(MAX_RECOMMENDATION)
            .map(x => Recommendation(x._1, x._2)))
      }
      .toDF()
    simDF.show()
    OperateMongoDb.dataFrameToSave(simDF, ITEM_CF_PRODUCT_RECS)

    simDF.unpersist()
    ratingDF.unpersist()
    sparkSession.stop()

  }

  // 按照公式计算同现相似度
  def cooccurrenceSim(coCount: Long, count1: Long, count2: Long): Double = {
    coCount / math.sqrt(count1 * count2)
  }
}
