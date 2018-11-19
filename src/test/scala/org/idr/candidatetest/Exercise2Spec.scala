package org.idr.candidatetest

import org.apache.spark.sql.DataFrame
import org.scalatest.Matchers

class Exercise2Spec extends SparkSessionSpec with Matchers {

  val crimeStatisticsPath = getClass.getResource("/exercise2").getFile()
  val service = new Exercise2(readCSV(crimeStatisticsPath))

  test("Zip codes with most crime are not empty") {
    val outputPath = s"$parquetOutputDir/zipCodesWithMostCrime"

    service.getZipCodesWithMostCrime(outputPath)

    val actual = getParquet(outputPath)
    service.zipCodesLimit shouldEqual actual.count()
    1 shouldEqual actual.columns.size
    val firstZipCode: String = actual.first().getAs(0)
    assert(firstZipCode.trim().length > 1)
  }

  test("Most violations are not empty") {
    val outputPath = s"$parquetOutputDir/mostViolations"

    service.getMostViolation(getZipCodesDF(), outputPath)

    val actual = getParquet(outputPath)
    assert(actual.count() == 1)
    val crime: String = actual.first().getAs(0)
    println(s" Most often crime is: $crime")
  }

  test("Crime count is one") {
    val outputPath = s"$parquetOutputDir/crimeCount"

    service.getCrimeCount(outputPath)
    val actual = getParquet(outputPath)

    1 shouldEqual actual.count()
  }

  test("Most dangerous month value is positive") {
    val outputPath = s"$parquetOutputDir/mostDangerousMonth"

    service.getMostDangerousMonth(outputPath)

    val actual = getParquet(outputPath)
    assert(actual.count() == 1)
    val month: Integer = actual.first().getAs(0)
    assert(month >= 0)
  }

  def getZipCodesDF(): DataFrame = {
    val zipCodesPath = s"$parquetOutputDir/zipCodesDF"
    service.getZipCodesWithMostCrime(zipCodesPath)
    getParquet(zipCodesPath)
  }

}
