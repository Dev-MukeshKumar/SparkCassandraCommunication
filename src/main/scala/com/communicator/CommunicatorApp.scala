package com.communicator

import com.datastax.spark.connector.{SomeColumns, toRDDFunctions, toSparkContextFunctions}
import data.schema.{PurchaseInfo, UserHistory}
import data.generate.IndividualData._
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.util.Properties
import scala.io.Source

object CommunicatorApp extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args:Array[String]):Unit = {

    logger.info("Started Communicator App!")

    val spark = SparkSession.builder()
      .config(getSparkConf)
      .getOrCreate()

    //read data as RDD from DB
    val readData = spark.sparkContext.cassandraTable[UserHistory]("cassandra_communication","countrywise_sales")
    logger.info("Info to cross verify read from DB. Total rows: "+ readData.count())

    //read data as RDD from DB with constrains and few columns
    val userNameAndPurchasesData = spark.sparkContext.cassandraTable[(String, List[PurchaseInfo])]("cassandra_communication","countrywise_sales")
      .select("username","purchases").where("country = 'India'")
    logger.info("Sample data of read (username,purchases):")
    userNameAndPurchasesData.take(3).foreach(logger.info(_))

    //read data as RDD from DB with constrains and perform operations
    val dbData = spark.sparkContext.cassandraTable[UserHistory]("cassandra_communication","countrywise_sales").where("country in ('India','Japan','Nepal','Pakistan','Germany')").cache()

    //1. total price of purchase of each user
    val totalPriceEachUserData = dbData.map(data => (data.country,data.username,data.purchases.map(_.item_price).sum))
    logger.info("total price of purchases of each user:")
    totalPriceEachUserData.take(5).foreach(logger.info(_))

    //max and min price per country
    val totalPricePerCountry = totalPriceEachUserData.map(data => (data._1,(data._2,data._3)))
    logger.info("Max price purchased user in a country: ")
    val maxPriceUserCountryWise = totalPricePerCountry.reduceByKey((v1,v2)=> if(v1._2 > v2._2) v1 else v2)
    maxPriceUserCountryWise.foreach(logger.info(_))
    logger.info("Min price purchased user in a country: ")
    val minPriceUserCountryWise = totalPricePerCountry.reduceByKey((v1,v2)=> if(v1._2 < v2._2) v1 else v2)
    minPriceUserCountryWise.foreach(logger.info(_))


    //read from DB using join operation
    val countriesRdd = spark.sparkContext.parallelize(country.map(data => UserHistory(Option(data),None,None,None,List.empty,None)))
    val readDataByJoinOperation = countriesRdd.joinWithCassandraTable[UserHistory]("cassandra_communication","countrywise_sales").on(SomeColumns("country")).cache()
    val readDataByJoin = readDataByJoinOperation.map(_._2)
    logger.info("No. of row after join: "+readDataByJoin.count())
    logger.info("Sample data of read data by join: ")
    readDataByJoin.take(10).foreach(logger.info(_))

    spark.stop()
    logger.info("Shutting down Communicator app!")
  }

  def getSparkConf(): SparkConf = {
    val sparkConf = new SparkConf
    val props = new Properties
    props.load(Source.fromFile("spark.conf").reader())
    props.forEach((k, v) => sparkConf.set(k.toString, v.toString))
    sparkConf
  }
}
