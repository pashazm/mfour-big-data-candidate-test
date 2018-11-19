package org.idr.candidatetest

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FunSuite}

abstract class SparkSessionSpec extends FunSuite with BeforeAndAfterAll {

  val spark = SparkSession
    .builder
    .appName(getClass().getSimpleName)
    .master("local[2]")
    .getOrCreate()


  val parquetOutputDir = "target/" + getClass().getSimpleName

  override def beforeAll() {
    FileUtils.deleteDirectory(new File(parquetOutputDir))
  }

  def readCSV(filePath: String): DataFrame = {
    spark.read.option("header", "true").csv(filePath)
  }

  def getParquet(path: String): DataFrame = {
    spark.read.parquet(path)
  }

}
