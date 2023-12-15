package com.niit.recommender

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *@author Gary Chen
 *@description 导入数据
 *@date 2023/12/14 22:25
 *@project ECommerceRecommendSystem
 *
 **/


/**
  * Product数据集
  * 0000791156                                                    商品ID
  * Spirit Led&mdash;Moving By Grace In The Holy Spirit's Gifts   商品名称
  * https://images-na.ssl-images-ama                              商品的图品URL
  * Movies & TV Movies                                            商品分类
  */

case class Product( productId: Int, name: String, imageUrl: String, categories: String)

/**
  * Rating数据集
  * 174744            用户ID
  * 1526863           商品ID
  * 5.0               评分
  * 1458000000        时间戳
  */

case class Rating( userId: Int, productId: Long, score: Double, timestamp: Int )


case class MongoConfig( uri: String, db: String )

object DataLoader {
  // 定义数据文件路径
  val PRODUCT_DATA_PATH = "recommender/DataLoader/src/main/resources/movies&tv/products.csv"
  val RATING_DATA_PATH = "recommender/DataLoader/src/main/resources/movies&tv/ratings.csv"
  // 定义mongodb中存储的表名
  val MONGODB_PRODUCT_COLLECTION = "Product"
  val MONGODB_RATING_COLLECTION = "Rating"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
//      "spark.executor.memory" -> "4g", // Set the amount of memory each executor can use
//      "spark.driver.memory" -> "2g", // Set the amount of memory the driver can use
      "mongo.uri" -> "mongodb://niit-master:27017/my_recommender",
      "mongo.db" -> "my_recommender"
    )
    // 创建一个spark config
    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")
    // 创建spark session
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    // 加载数据
    val productRDD = spark.sparkContext.textFile(PRODUCT_DATA_PATH)
    val productDF = productRDD.map( item => {
      // product数据通过^分隔，切分出来
      val attr = item.split("\\^")
      // 转换成Product
      Product( attr(0).toInt, attr(1).trim, attr(2).trim, attr(3).trim )
    } ).toDF()
//    productDF.show()

    val ratingRDD = spark.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDF = ratingRDD.map(item => {
      val attr = item.split(",")
      Rating(attr(0).toInt, attr(1).toLong, attr(2).toDouble, attr(3).toInt)
    }).toDF()

    implicit val mongoConfig = MongoConfig( config("mongo.uri"), config("mongo.db") )
    storeDataInMongoDB( productDF, ratingDF )

    spark.stop()
  }
  def storeDataInMongoDB( productDF: DataFrame, ratingDF: DataFrame )(implicit mongoConfig: MongoConfig): Unit ={
    // 新建一个mongodb的连接，客户端
    val mongoClient = MongoClient( MongoClientURI(mongoConfig.uri) )
    // 定义要操作的mongodb表，可以理解为 db.Product
    val productCollection = mongoClient( mongoConfig.db )( MONGODB_PRODUCT_COLLECTION )
    val ratingCollection = mongoClient( mongoConfig.db )( MONGODB_RATING_COLLECTION )

    // 如果表已经存在，则删掉
    productCollection.dropCollection()
    ratingCollection.dropCollection()

    // 将当前数据存入对应的表中
    productDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_PRODUCT_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 对表创建索引
    productCollection.createIndex( MongoDBObject( "productId" -> 1 ) )
    ratingCollection.createIndex( MongoDBObject( "productId" -> 1 ) )
    ratingCollection.createIndex( MongoDBObject( "userId" -> 1 ) )

    mongoClient.close()
  }
}
