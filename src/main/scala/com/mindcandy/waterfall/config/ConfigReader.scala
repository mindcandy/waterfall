package com.mindcandy.waterfall.config

import scalaz._
import com.typesafe.config.{ConfigRenderOptions, Config}
import com.mindcandy.waterfall.actor.Protocol.{DropJob, DropJobList}
import argonaut._, Argonaut._
import com.mindcandy.waterfall.actor.Protocol._

trait ConfigReader {
  def jobsDatabaseConfig: Reader[Config, JobsDatabaseConfig] = Reader( config => {
    val jsonString: String = config.getList("waterfall.dropJobList").render(ConfigRenderOptions.concise())
    JobsDatabaseConfig(DropJobList(jsonString.decodeOption[List[DropJob]].get))
  })
}
