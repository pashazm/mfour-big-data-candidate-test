package org.idr.candidatetest

import org.apache.spark.sql.Encoders
import org.scalatest.Matchers

import scala.io.Source

class Exercise1Spec extends SparkSessionSpec with Matchers {

  val realEstateStatisticsPath = getClass.getResource("/exercise1/Sacramentorealestatetransactions.csv").getFile()
  val crimeStatisticsPath = getClass.getResource("/exercise1/SacramentocrimeJanuary2006.csv").getFile()

  val service = new Exercise1(readCSV(realEstateStatisticsPath), readCSV(crimeStatisticsPath))

  test("Sold houses number should equal line number in sales statistic file, minus header") {
    val outputPath = s"$parquetOutputDir/soldHouses"

    service.getSoldHousesCount(outputPath)

    getFirstLongValue(outputPath) shouldEqual getCSVLinesCount(realEstateStatisticsPath)
  }

  test("Crime number should equal line number in crime statistics file, minus header") {
    val outputPath = s"$parquetOutputDir/crimesCount"

    service.getCrimesCount(outputPath)

    getFirstLongValue(outputPath) shouldEqual getCSVLinesCount(crimeStatisticsPath)
  }

  test("Crime number in sold household is more then 1") {
    val outputPath = s"$parquetOutputDir/crimesAtHouseholdCount"

    service.getCrimesAtHouseholdCount(outputPath)

    assert(getFirstLongValue(outputPath) > 1)
  }

  test("Crimes in sold households has 'Burglary' ") {
    val outputPath = s"$parquetOutputDir/crimesAtHouseholdDescription"

    service.getCrimesAtHouseholdDescription(outputPath)

    val descriptions = getParquet(outputPath).as(Encoders.STRING).collect()
    val burglary = descriptions.find(_.toLowerCase.contains("burglary"))
    assume(burglary.isDefined)
  }


  private def getCSVLinesCount(filePath: String): Int = {
    Source.fromFile(filePath).getLines().size - 1
  }

  private def getFirstLongValue(dataFramePath: String): Long = {
    getParquet(dataFramePath).first().getAs(0)
  }

}
