package org.idr.candidatetest

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType


class Exercise2(crimeStatisticsDF: DataFrame) {

  val zipCodesLimit = 10

  def getZipCodesWithMostCrime(outputDir: String): Unit = {
    val result = crimeStatisticsDF
      .groupBy("OccurenceZipCode")
      .count()
      .orderBy(col("count").desc)
      .drop("count")
      .limit(zipCodesLimit)

    toParquet(result, outputDir)
  }

  def getMostViolation(zipCodesDF: DataFrame, outputDir: String): Unit = {
    val result = crimeStatisticsDF
      .join(zipCodesDF, Seq("OccurenceZipCode"))
      .groupBy("PrimaryViolation")
      .count()
      .orderBy(col("count").desc)
      .drop("count")
      .limit(1)

    toParquet(result, outputDir)
  }

  def getCrimeCount(outputDir: String): Unit = {
    val result = crimeStatisticsDF.select(count("*").alias("cnt"))
    toParquet(result, outputDir)
  }

  def getMostDangerousMonth(outputDir: String): Unit = {
    val monthColumn = month(unix_timestamp(col("ReportDate"), "M/dd/yyyy HH:mm").cast(TimestampType))
    val monthDF = crimeStatisticsDF.select(monthColumn.alias("month"))
    val result = monthDF
      .groupBy("month")
      .count()
      .orderBy(col("count").desc)
      .drop("count")
      .limit(1)
    toParquet(result, outputDir)
  }


  private def toParquet(df: DataFrame, outputDir: String): Unit = {
    df.write.parquet(outputDir)
  }

}
