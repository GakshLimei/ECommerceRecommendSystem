package com.niit.offline

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix

/**
 *@author Gary Chen
 *@description 
 *@date 2023/12/15 03:54
 *@project 
 * 
 **/

case class ProductRating( userId: Int, productId: Long, score: Double, timestamp: Int )
case class MongoConfig( uri: String, db: String )

// 定义标准推荐对象
case class Recommendation( productId: Long, score: Double )
// 定义用户的推荐列表
case class UserRecs( userId: Int, recs: Seq[Recommendation] )
// 定义商品相似度列表
case class ProductRecs( productId: Long, recs: Seq[Recommendation] )

object OfflineRecommender {
  // 定义mongodb中存储的表名
  val MONGODB_RATING_COLLECTION = "Rating"

  val USER_RECS = "UserRecs"
  val PRODUCT_RECS = "ProductRecs"
  val USER_MAX_RECOMMENDATION = 20

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.master" -> "spark://192.168.10.102:7077",  // Spark Master的地址
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://niit-master:27017/my_recommender",
      "mongo.db" -> "my_recommender"
    )
    // 创建一个spark config
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")
    // 创建spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._
    implicit val mongoConfig = MongoConfig( config("mongo.uri"), config("mongo.db") )

    // 加载数据
    val ratingRDD = spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRating]
      .rdd
      .map(
        rating => (rating.userId, rating.productId, rating.score)
      ).cache()

    // 提取出所有用户和商品的数据集
    val userRDD = ratingRDD.map(_._1).distinct()
    val productRDD = ratingRDD.map(_._2).distinct()

    // 核心计算过程
    // 1. 训练隐语义模型
    val trainData = ratingRDD.map(x=>Rating(x._1,x._2.toInt,x._3))
    // 定义模型训练的参数，rank隐特征个数，iterations迭代词数，lambda正则化系数
    val ( rank, iterations, lambda ) = ( 5, 10, 0.01 )
    val model = ALS.train( trainData, rank, iterations, lambda )

    // 2. 获得预测评分矩阵，得到用户的推荐列表
    // 用userRDD和productRDD做一个笛卡尔积，得到空的userProductsRDD表示的评分矩阵
    // Assuming userProducts is an RDD[(Int, Long)]
    val userProducts = userRDD.cartesian(productRDD).map { case (user, product) => (user, product.toInt) }

    // Now userProducts is of type RDD[(Int, Int)], and you can use it with model.predict
    val preRating = model.predict(userProducts)


    // 从预测评分矩阵中提取得到用户推荐列表
    val userRecs = preRating.filter(_.rating>0)
      .map(
        rating => ( rating.user, ( rating.product, rating.rating ) )
      )
      .groupByKey()
      .map{
        case (userId, recs) =>
          UserRecs( userId, recs.toList.sortWith(_._2>_._2).take(USER_MAX_RECOMMENDATION).map(x=>Recommendation(x._1,x._2)) )
      }
      .toDF()
    userRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 3. 利用商品的特征向量，计算商品的相似度列表
    val productFeatures = model.productFeatures.map{
      case (productId, features) => ( productId, new DoubleMatrix(features) )
    }
    // 两两配对商品，计算余弦相似度
    val productRecs = productFeatures.cartesian(productFeatures)
      .filter{
        case (a, b) => a._1 != b._1
      }
      // 计算余弦相似度
      .map{
        case (a, b) =>
          val simScore = consinSim( a._2, b._2 )
          ( a._1, ( b._1, simScore ) )
      }
      .filter(_._2._2 > 0.4)
      .groupByKey()
      .map{
        case (productId, recs) =>
          ProductRecs( productId, recs.toList.sortWith(_._2>_._2).map(x=>Recommendation(x._1,x._2)) )
      }
      .toDF()
    productRecs.write
      .option("uri", mongoConfig.uri)
      .option("collection", PRODUCT_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    spark.stop()
  }
  def consinSim(product1: DoubleMatrix, product2: DoubleMatrix): Double ={
    product1.dot(product2)/ ( product1.norm2() * product2.norm2() )
  }
}
