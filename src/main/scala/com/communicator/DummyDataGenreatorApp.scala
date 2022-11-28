package com.communicator

import data.generate.TableData._
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import data.schema._
import data.generate.IndividualData._
import org.apache.spark.sql.functions.slice

import java.util.Properties
import scala.io.Source

object DummyDataGeneratorApp extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    logger.info("Started DummyDataGenerator App!")

    val spark = SparkSession.builder()
      .config(getSparkConf)
      .getOrCreate()



    //1. random data generate and write ot DB
    import spark.sqlContext.implicits._
    val generatedData: Dataset[UserHistory] = userHistoryList(20000).toDS()
    generatedData.write.format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace"->"cassandra_communication","table"->"countrywise_sales"))
      .mode("append")
      .save()

    //2. read all data then modify some data and update back to DB
    val readData = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("keyspace"->"cassandra_communication","table"->"countrywise_sales"))
      .load.as[UserHistory]

    logger.info(readData.count())

    val phoneNumberNullData = makePhoneNumberNull(spark,randomNumberBetween(0,19900),readData)

    val ageNullData = makeAgeNull(spark,randomNumberBetween(0,19900),phoneNumberNullData)

    val purchaseEmptyListData = makePurchasesEmptyList(spark,randomNumberBetween(0,19900),ageNullData)

    val usernameNullData = makeUserNameNull(spark,randomNumberBetween(0,19500),purchaseEmptyListData)

    val finalDS = usernameNullData.toSeq.toDS()

    logger.info(finalDS.count())

    finalDS.write.format("org.apache.spark.sql.cassandra")
          .option("confirm.truncate","true")
          .options(Map("keyspace"->"cassandra_communication","table"->"countrywise_sales"))
          .mode("overwrite")
          .save()

    spark.stop()
    logger.info("Shutting down DummyDataGenerator app!")
  }

  def getSparkConf(): SparkConf = {
    val sparkConf = new SparkConf
    val props = new Properties
    props.load(Source.fromFile("spark.conf").reader())
    props.forEach((k, v) => sparkConf.set(k.toString, v.toString))
    sparkConf
  }

  def makePhoneNumberNull(spark:SparkSession,startIndex:Int, data: Dataset[UserHistory]): Array[UserHistory] = {
    import spark.implicits._
    data.map(row => row.id match {
      case Some(n) if n >= startIndex && n < (startIndex+100) => UserHistory(row.country,row.id,row.age,None,row.purchases,row.username)
      case Some(n) => row
    }).collect()
  }

  def makeAgeNull(spark: SparkSession, startIndex: Int, data: Array[UserHistory]): Array[UserHistory] = {
    import spark.implicits._
    data.map(row => row.id match {
      case Some(n) if n >= startIndex && n < (startIndex + 100) => UserHistory(row.country, row.id, None, row.phonenumber, row.purchases, row.username)
      case Some(n) => row
    })
  }

  def makePurchasesEmptyList(spark: SparkSession, startIndex: Int, data: Array[UserHistory]): Array[UserHistory] = {
    import spark.implicits._
    data.map(row => row.id match {
      case Some(n) if n >= startIndex && n < (startIndex + 100) => UserHistory(row.country, row.id, row.age, row.phonenumber, List.empty, row.username)
      case Some(n) => row
    })
  }

  def makeUserNameNull(spark: SparkSession, startIndex: Int, data: Array[UserHistory]): Array[UserHistory] = {
    import spark.implicits._
    data.map(row => row.id match {
      case Some(n) if n >= startIndex && n < (startIndex + 500) => UserHistory(row.country, row.id, row.age, row.phonenumber, row.purchases, None)
      case Some(n) => row
    })
  }

}


