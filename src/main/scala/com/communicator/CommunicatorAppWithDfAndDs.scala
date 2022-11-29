package com.communicator

import data.schema._
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.Properties
import scala.io.Source

object CommunicatorAppWithDfAndDs extends Serializable {

  @transient lazy val logger:Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]):Unit = {
    logger.info("Communicator app started!")

    val spark = SparkSession.builder().config(getSparkConf()).getOrCreate()
    import spark.implicits._

    //read data as DF and convert to DS[UserHistory]

    val dataDf = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace"->"cassandra_communication","table"->"countrywise_sales"))
      .load

    val dataDs = dataDf.as[UserHistory]
    logger.info("Sample data read as DS from DB:")
    dataDs.take(5).foreach(logger.info(_))

    //read data as DS from DB with constrains and some columns only
    val userPurchaseDs = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace" -> "cassandra_communication", "table" -> "countrywise_sales"))
      .load
      .select("username","purchases")
      .where("country='India'")
      .as[(Option[String],List[PurchaseInfo])]

    logger.info("")
    logger.info("Sample data read as DS from DB with constrains and few columns:")
    userPurchaseDs.take(5).foreach(logger.info(_))

    //read data from DB and apply manipulations
    val parentDataDs = spark.read.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace"->"cassandra_communication","table"->"countrywise_sales"))
      .load
      .where("country in ('India','Japan','Nepal','Germany','Pakistan')")
      .as[UserHistory].cache()

    logger.info(parentDataDs.count())

    //2. total purchase price for each user
    val totalPurchasePriceEachUserDs = parentDataDs.map(row => totalPurchasePriceOfUser(row.country,row.username,row.purchases.map(_.item_price).sum))
    logger.info("")
    logger.info("Sample data of total price of purchase for each user: ")
    totalPurchasePriceEachUserDs.take(5).foreach(logger.info(_))


    //1. max and min price of purchase in a country
    val maxPurchasePriceInCountry = totalPurchasePriceEachUserDs.groupBy("country").max("totalPrice").as[(String,Int)]
    logger.info("")
    logger.info("Max purchase price in a country:")
    maxPurchasePriceInCountry.foreach(logger.info(_))

    val minPurchasePriceInCountry = totalPurchasePriceEachUserDs.groupBy("country").min("totalPrice").as[(String,Int)]
    logger.info("")
    logger.info("Min purchase price in a country:")
    minPurchasePriceInCountry.foreach(logger.info(_))

    logger.info("")
    logger.info("Communicator app shutting down!")
  }

  def getSparkConf(): SparkConf = {
    val sparkConf = new SparkConf
    val properties = new Properties
    properties.load(Source.fromFile("spark.conf").reader())
    properties.forEach((k,v) => sparkConf.set(k.toString,v.toString))
    sparkConf
  }
}
