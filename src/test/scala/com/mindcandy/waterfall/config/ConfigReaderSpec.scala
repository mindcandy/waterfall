package com.mindcandy.waterfall.config

import com.typesafe.config.ConfigFactory
import org.specs2.specification.Grouped
import org.specs2.specification.script.Specification

import scala.concurrent.duration._

class ConfigReaderSpec extends Specification with Grouped with ConfigReader {
  def is = s2"""
  ConfigReader test
  ==============================================================================
    read dropFactoryClass ${dropFactoryClassReader}
    read maxScheduleTime ${maxScheduleTimeReader}
    read checkJobsPeriod ${checkJobsPeriodReader}
    read databaseURL ${databaseURLReader}
    read databaseUsername ${databaseUsernameReader}
    read databasePassword ${databasePasswordReader}
    read allowJobsRunInParallel ${allowJobsRunInParallelReader}
  """

  val config = ConfigFactory.parseString(
    """
      |waterfall {
      |  database: {
      |    url: "jdbc:h2:./test.db"
      |    username: "user"
      |    password: "pw"
      |  }
      |  maxScheduleTimeInMinutes: 60
      |  checkJobsPeriodInMinutes: 1
      |  dropFactoryClass: com.mindcandy.waterfall.TestWaterfallDropFactory
      |  allowJobsRunInParallel: false
      |}
    """.stripMargin
  )

  def dropFactoryClassReader = dropFactoryClass(config) must_== "com.mindcandy.waterfall.TestWaterfallDropFactory"

  def maxScheduleTimeReader = maxScheduleTime(config) must_== FiniteDuration(60, MINUTES)

  def checkJobsPeriodReader = checkJobsPeriod(config) must_== FiniteDuration(1, MINUTES)

  def databaseURLReader = databaseURL(config) must_== "jdbc:h2:./test.db"

  def databaseUsernameReader = databaseUsername(config) must_== "user"

  def databasePasswordReader = databasePassword(config) must_== "pw"

  def allowJobsRunInParallelReader = allowJobsRunInParallel(config) must beFalse
}
