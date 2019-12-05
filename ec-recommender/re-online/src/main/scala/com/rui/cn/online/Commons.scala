package com.rui.cn.online

import com.mongodb.casbah.commons.MongoDBObject

import scala.collection.JavaConversions._

object Commons {
  /**
    * 流式的推荐结果
    */
  val MONGODB_STREAM_RECS_COLLECTION = "StreamRecs"
  /**
    * 用户评分表
    */
  val MONGODB_RATING_COLLECTION = "Rating"
  /**
    * 商品相似度矩阵
    */
  val MONGODB_Product_RECS_COLLECTION = "ProductRecs"

  /**
    * 获取当前最近的M次商品评分
    *
    * @param num    评分的个数
    * @param userId 谁的评分
    * @return
    */
  def getUserRecentlyRating(num: Int, userId: Int): Array[(Int, Double)] = {
    //从用户的队列中取出num个商品评论 userId:userId
    RedisUtil.pool.getResource.lrange("userId:" + userId.toString, 0, num).map { item =>
      val attr = item.split("\\:")
      (attr(0).trim.toInt, attr(1).trim.toDouble)
    }.toArray
  }

  /**
    * 获取当前商品K个相似的商品
    *
    * @param num         相似商品的数量
    * @param productId   当前商品的ID
    * @param userId      当前的评分用户
    * @param simProducts 商品相似度矩阵的广播变量值
    * @param mongoConfig MongoDB的配置
    * @return
    */
  def getTopSimProducts(num: Int, productId: Int, userId: Int, simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])(implicit mongoConfig: MongoConfig): Array[Int] = {
    //从广播变量的商品相似度矩阵中获取当前商品所有的相似商品
    val allSimProducts: Array[(Int, Double)] = simProducts.get(productId).map(_.toArray).get

    // 获取用户已经看过的商品
    val hasRating: Array[Int] = RedisUtil.mongoClient(mongoConfig.dbName)(MONGODB_RATING_COLLECTION).find(MongoDBObject("userId" -> userId)).toArray.map {
      item => item.get("productId").toString.toInt
    }

    // 过滤掉已经评分过得商品，并排序输出
    allSimProducts.filter(x => !hasRating.contains(x._1)).sortWith(_._2 > _._2).take(num).map(x => x._1)

  }

  /**
    * 计算待选商品的推荐分数
    *
    * @param simProducts         商品相似度矩阵 map[productId,map[productId,score]]
    * @param userRecentlyRatings 用户最近的k次评分  (productId,score)
    * @param topSimProducts      当前商品最相似的K个商品(productId)
    * @return
    */
  def computeProductScores(simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]], userRecentlyRatings: Array[(Int, Double)], topSimProducts: Array[Int]): Array[(Int, Double)] = {
    //用于保存每一个待选商品和最近评分的每一个商品的权重得分
    val score = scala.collection.mutable.ArrayBuffer[(Int, Double)]()

    //用于保存每一个商品的增强因子数
    val increMap = scala.collection.mutable.HashMap[Int, Int]()

    //用于保存每一个商品的减弱因子数
    val decreMap = scala.collection.mutable.HashMap[Int, Int]()

    for (topSimProduct <- topSimProducts; userRecentlyRating <- userRecentlyRatings) {
      // 加权计算 获取基础评分
      val simScore = getProductsSimScore(simProducts, userRecentlyRating._1, topSimProduct)
      if (simScore > 0.6) {
        score += ((topSimProduct, simScore * userRecentlyRating._2))
        if (userRecentlyRating._2 > 3) {
          increMap(topSimProduct) = increMap.getOrDefault(topSimProduct, 0) + 1
        } else {
          decreMap(topSimProduct) = decreMap.getOrDefault(topSimProduct, 0) + 1
        }
      }
    }

    score.groupBy(_._1).map {
      case (productId, sims) =>
        (productId, sims.map(_._2).sum / sims.length + log(increMap.getOrDefault(productId, 1)) - log(decreMap.getOrDefault(productId, 1)))
    }.toArray
      .sortWith(_._2 > _._2)

  }

  /**
    * 获取单个商品之间的相似度
    *
    * @param simProducts       商品相似度矩阵
    * @param userRatingProduct 用户已经评分的商品id
    * @param topSimProduct     候选商品的id
    * @return
    */
  def getProductsSimScore(simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]], userRatingProduct: Int, topSimProduct: Int): Double = {
    simProducts.get(topSimProduct) match {
      case Some(sim) => sim.get(userRatingProduct) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }
  }

  //取2的对数
  def log(m: Int): Double = {
    math.log(m) / math.log(2)
  }

}
