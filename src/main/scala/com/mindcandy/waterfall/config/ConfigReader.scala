package com.mindcandy.waterfall.config

import argonaut.Argonaut._
import argonaut._
import com.mindcandy.waterfall.actor.Protocol._
import com.typesafe.config.{ Config, ConfigRenderOptions }

import scala.concurrent.duration._
import scalaz._

trait ConfigReader {
  def dropFactoryClass: Reader[Config, String] = Reader(config => config.getString("waterfall.dropFactoryClass"))

  def maxScheduleTime: Reader[Config, FiniteDuration] = Reader(config => FiniteDuration(config.getInt("waterfall.maxScheduleTimeInMinutes"), MINUTES))

  def checkJobsPeriod: Reader[Config, FiniteDuration] = Reader(config => FiniteDuration(config.getInt("waterfall.checkJobsPeriodInMinutes"), MINUTES))

  def logDatabase: Reader[Config, String] = Reader(
    config => config.getString("waterfall.database.url")
  )

  def username: Reader[Config, String] = Reader(
    config => config.getString("waterfall.database.username")
  )

  def password: Reader[Config, String] = Reader(
    config => config.getString("waterfall.database.password")
  )
}
