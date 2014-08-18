package com.mindcandy.waterfall.config

import argonaut.Argonaut._
import argonaut._
import com.mindcandy.waterfall.actor.Protocol._
import com.typesafe.config.{ Config, ConfigRenderOptions }

import scala.concurrent.duration._
import scalaz._

trait ConfigReader {
  def jobsDatabaseConfig: Reader[Config, JobsDatabaseConfig] = Reader(config => {
    val jsonString: String = config.getList("waterfall.dropJobList").render(ConfigRenderOptions.concise())
    val dropJobs = jsonString.decodeEither[List[DropJob]] match {
      case -\/(error) => throw new IllegalArgumentException(error)
      case \/-(dropJobs) => dropJobs
    }
    JobsDatabaseConfig(
      DropJobList(dropJobs.map(x => x.jobID.getOrElse(0) -> x).toMap))
  })

  def dropFactoryClass: Reader[Config, String] = Reader(config => config.getString("waterfall.dropFactoryClass"))

  def maxScheduleTime: Reader[Config, FiniteDuration] = Reader(config => FiniteDuration(config.getInt("waterfall.maxScheduleTimeInMinutes"), MINUTES))

  def checkJobsPeriod: Reader[Config, FiniteDuration] = Reader(config => FiniteDuration(config.getInt("waterfall.checkJobsPeriodInMinutes"), MINUTES))

  def logDatabase: Reader[Config, String] = Reader(
    config => config.getString("waterfall.logDatabase")
  )

  def username: Reader[Config, String] = Reader(
    config => config.getString("waterfall.username")
  )

  def password: Reader[Config, String] = Reader(
    config => config.getString("waterfall.password")
  )
}
