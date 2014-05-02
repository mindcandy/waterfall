package com.mindcandy.waterfall.actors

import org.joda.time.DateTime
import argonaut._
import Argonaut._

object Protocol {
  case class DropJob(jobId: Int, name: String, cron: String, dropUID: String)
  case class DropJobList(jobs: List[DropJob])
  case class DropLog(logId: Int, jobId: Int, startTime: DateTime, endTime: Option[DateTime], logOutput: Option[String], exception: Option[Throwable])
  case class DropHistory(logs: List[DropLog])
  
  implicit def DropJobCodecJson: EncodeJson[DropJob] = casecodec4(DropJob.apply, DropJob.unapply)("jobId", "name", "cron", "dropUID")
}