package com.mindcandy.waterfall.actor

import akka.actor.Actor
import akka.actor.Props
import com.mindcandy.waterfall.actor.Protocol.{JobID, DropJob, DropJobList, DropLog}
import akka.actor.ActorLogging
import com.mindcandy.waterfall.config.JobsDatabaseConfig

object JobDatabaseManager {
  case class GetJobForCompletion(jobId: JobID, completionFunction: Option[DropJob] => Unit)
  case class GetScheduleForCompletion(completionFunction: Map[JobID, DropJob] => Unit)
  case class GetSchedule()

  def props(config: JobsDatabaseConfig, db: DB): Props = Props(new JobDatabaseManager(config.dropJobList, db))
}

class JobDatabaseManager(dropJobList: DropJobList, db: DB) extends Actor with ActorLogging {
  import JobDatabaseManager._

  def receive = {
    case GetJobForCompletion(jobId, f) => {
      log.debug(s"job lookup for id $jobId")
      val result = dropJobList.jobs.lift(jobId)
      f(result)
    }
    case GetScheduleForCompletion(f) => {
      log.debug(s"schedule lookup for completion")
      f(dropJobList.jobs)
    }
    case GetSchedule() => {
      log.debug(s"schedule lookup")
      sender ! dropJobList
    }
    case dropLog: DropLog => {
      log.debug(s"drop log received")
      db.insert(db.dropLogs, dropLog)
    }
  }
}