package com.mindcandy.waterfall.config

import com.typesafe.config.Config

import scala.concurrent.duration._
import scalaz._

trait ConfigReader {
  def dropFactoryClass: Reader[Config, String] = Reader(config => config.getString("waterfall.dropFactoryClass"))

  def maxScheduleTime: Reader[Config, FiniteDuration] = Reader(config => FiniteDuration(config.getInt("waterfall.maxScheduleTimeInMinutes"), MINUTES))

  def checkJobsPeriod: Reader[Config, FiniteDuration] = Reader(config => FiniteDuration(config.getInt("waterfall.checkJobsPeriodInMinutes"), MINUTES))

  def databaseURL: Reader[Config, String] = Reader(
    config => config.getString("waterfall.database.url")
  )

  def databaseUsername: Reader[Config, String] = Reader(
    config => config.getString("waterfall.database.username")
  )

  def databasePassword: Reader[Config, String] = Reader(
    config => config.getString("waterfall.database.password")
  )

  def allowJobsRunInParallel: Reader[Config, Boolean] = Reader(
    config => config.getBoolean("waterfall.allowJobsRunInParallel")
  )
}
