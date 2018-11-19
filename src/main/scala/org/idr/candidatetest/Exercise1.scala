package org.idr.candidatetest

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class Exercise1(realEstateStatisticsDf: DataFrame, crimeStatisticsDF: DataFrame) {

  def getSoldHousesCount(outputDir: String): Unit = {
    toParquet(getCount(realEstateStatisticsDf), outputDir)
  }

  def getCrimesCount(outputDir: String): Unit = {
    toParquet(getCount(crimeStatisticsDF), outputDir)
  }

  def getCrimesAtHouseholdCount(outputDir: String): Unit = {
    val result = getCount(getCrimesAtHouseHolds())
    toParquet(result, outputDir)
  }

  def getCrimesAtHouseholdDescription(outputDir: String): Unit = {
    val result = getCrimesAtHouseHolds().select("crimedescr")
    toParquet(result, outputDir)
  }

  private def getCount(df: DataFrame): DataFrame = {
    df.select(count("*").alias("cnt"))
  }

  private def getCrimesAtHouseHolds(): DataFrame = {
    realEstateStatisticsDf.join(crimeStatisticsDF, col("address") === col("street"))
  }

  private def toParquet(df: DataFrame, outputDir: String): Unit = {
    df.write.parquet(outputDir)
  }

}
