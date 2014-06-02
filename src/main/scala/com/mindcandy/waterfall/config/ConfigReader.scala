package com.mindcandy.waterfall.config

import scalaz._
import com.typesafe.config.{ConfigRenderOptions, Config}
import com.mindcandy.waterfall.actor.Protocol.{DropJob, DropJobList}
import argonaut._, Argonaut._
import com.mindcandy.waterfall.actor.Protocol._
import scala.concurrent.duration._

trait ConfigReader {
  def jobsDatabaseConfig: Reader[Config, JobsDatabaseConfig] = Reader( config => {
    val jsonString: String = config.getList("waterfall.dropJobList").render(ConfigRenderOptions.concise())
    JobsDatabaseConfig(DropJobList(jsonString.decodeEither[List[DropJob]] match {
      case -\/(error) => throw new IllegalArgumentException(error)
      case \/-(dropJobs) => dropJobs
    }))
  })

  def dropFactoryClass: Reader[Config, String] = Reader( config => config.getString("waterfall.dropFactoryClass"))

  def maxScheduleTime: Reader[Config, FiniteDuration] = Reader( config => FiniteDuration(config.getInt("waterfall.maxScheduleTimeInMinutes"), MINUTES))

  def checkJobsPeriod: Reader[Config, FiniteDuration] = Reader( config => FiniteDuration(config.getInt("waterfall.checkJobsPeriodInMinutes"), MINUTES))
}
