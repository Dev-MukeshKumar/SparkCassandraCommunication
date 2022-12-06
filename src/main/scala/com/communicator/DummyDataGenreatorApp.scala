package com.communicator

import data.generate.TableData._
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import data.schema._
import data.generate.IndividualData._

import java.util.Properties
import scala.io.Source
import scala.collection.mutable.Set

object DummyDataGeneratorApp extends Serializable {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    logger.info("Started DummyDataGenerator App!")

    val spark = SparkSession.builder()
      .config(getSparkConf)
      .getOrCreate()

    import spark.sqlContext.implicits._

    //1. random data generate and write ot DB
    val generatedData: Dataset[UserHistory] = userHistoryList(20000).toDS()

    generatedData.repartition(6)

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

    val phoneNumberNullData = makePhoneNumberNull(spark,readData)

    val ageNullData = makeAgeNull(spark,phoneNumberNullData)

    val purchaseEmptyListData = makePurchasesEmptyList(spark,ageNullData)

    val usernameNullData = makeUserNameNull(spark,purchaseEmptyListData)

    usernameNullData.write.format("org.apache.spark.sql.cassandra")
          .options(Map("keyspace"->"cassandra_communication","table"->"countrywise_sales"))
          .mode("append")
          .save()

//    //testing
//    val name = readData.filter(row => row.username.isEmpty).count()
//    val age = readData.filter(row => row.age.isEmpty).count()
//    val purchase = readData.filter(row => row.purchases.isEmpty).count()
//    val phone = readData.filter(row => row.phonenumber.isEmpty).count()
//    logger.info(name,age,purchase,phone)
//    //op: (500,100,100,100)

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

  def createSet(n:Int,set:Set[Int]=Set()): Set[Int] = {
    if(set.size>=n) set
    else createSet(n,set+= randomNumberBetween(1,20000))
  }

  def makePhoneNumberNull(spark: SparkSession, data: Dataset[UserHistory]): Dataset[UserHistory] = {
    import spark.implicits._
    val dataSetToUpdate = createSet(100)
    data.map(row => if (dataSetToUpdate.contains(row.id.get)) UserHistory(row.country, row.id, row.age, None, row.purchases, row.username) else row)
  }

  def makeAgeNull(spark: SparkSession, data: Dataset[UserHistory]): Dataset[UserHistory] = {
    import spark.implicits._
    val dataSetToUpdate = createSet(100)
    data.map(row =>if(dataSetToUpdate.contains(row.id.get)) UserHistory(row.country, row.id, None, row.phonenumber, row.purchases, row.username) else row)
  }

  def makePurchasesEmptyList(spark: SparkSession, data: Dataset[UserHistory]): Dataset[UserHistory] = {
    import spark.implicits._
    val dataSetToUpdate = createSet(100)
    data.map(row => if(dataSetToUpdate.contains(row.id.get)) UserHistory(row.country, row.id, row.age, row.phonenumber, List.empty, row.username) else row)
  }

  def makeUserNameNull(spark: SparkSession, data: Dataset[UserHistory]): Dataset[UserHistory] = {
    import spark.implicits._
    val dataSetToUpdate = createSet(500)
    data.map(row => if(dataSetToUpdate.contains(row.id.get)) UserHistory(row.country, row.id, row.age, row.phonenumber, row.purchases, None) else row)
  }

}


