package com.mindcandy.waterfall.actor

import akka.actor.{ Actor, ActorLogging, Props }
import com.mindcandy.waterfall.WaterfallDropFactory.DropUID
import com.mindcandy.waterfall.actor.Protocol.{ DropJob, DropJobList, DropLog, JobID }

import scala.slick.jdbc.JdbcBackend.Database.dynamicSession

object JobDatabaseManager {
  case class GetJobForCompletion(jobId: JobID, completionFunction: Option[DropJob] => Unit)
  case class GetJobsForCompletion(completionFunction: List[DropJob] => Unit)
  case class GetJobsWithDropUIDForCompletion(dropUID: DropUID, completionFunction: List[DropJob] => Unit)
  case class GetScheduleForCompletion(completionFunction: List[DropJob] => Unit)
  case class GetSchedule()
  case class PostJobForCompletion(dropJob: DropJob, completionFunction: Option[DropJob] => Unit)
  case class GetLogsForCompletion(jobID: Option[JobID], time: Option[Int], isException: Option[Boolean], completionFunction: List[DropLog] => Unit)
  case class PostLogForCompletion(dropLog: DropLog, completionFunction: Option[DropLog] => Unit)

  def props(db: DB): Props = Props(new JobDatabaseManager(db))
}

class JobDatabaseManager(db: DB) extends Actor with ActorLogging {
  import com.mindcandy.waterfall.actor.JobDatabaseManager._

  import scala.slick.driver.JdbcDriver.simple._

  def receive = {
    case GetJobForCompletion(jobId, f) => {
      log.debug(s"job lookup for id $jobId")
      f(db.executeInSession(db.dropJobs.filter(_.jobID === jobId).firstOption))
    }
    case GetJobsForCompletion(f) => {
      log.debug(s"Get all jobs")
      f(db.executeInSession(db.dropJobs.list))
    }
    case GetJobsWithDropUIDForCompletion(dropUID, f) => {
      log.debug(s"Get all jobs with dropUID: $dropUID")
      f(db.executeInSession(db.dropJobs.filter(_.dropUID === dropUID).list))
    }
    case GetScheduleForCompletion(f) => {
      log.debug(s"schedule lookup for completion")
      f(db.executeInSession(db.dropJobs.filter(_.enabled).list))
    }
    case GetSchedule() => {
      log.debug(s"schedule lookup")
      val dropJobs = db.executeInSession(db.dropJobs.list)
      sender ! DropJobList(dropJobs.map(x => x.jobID.getOrElse(-1) -> x).toMap)
    }
    case dropLog: DropLog => {
      log.debug(s"drop log received")
      db.insert(db.dropLogs, dropLog)
    }
    case PostJobForCompletion(dropJob, f) => {
      log.debug(s"Insert or update a job")
      f(db.executeInSession(db.insertOrUpdateDropJob(dropJob)))
    }
    case GetLogsForCompletion(jobID, time, isException, f) => {
      log.debug(s"Quesry logs for jobID:${jobID.getOrElse(None)}, time:${time.getOrElse(None)}, isException:${isException.getOrElse(None)}")
      f(db.executeInSession(db.selectDropLog(jobID, time, isException)))
    }
    case PostLogForCompletion(dropLog, f) => {
      log.debug("Insert a log")
      f(db.executeInSession(db.insertAndReturnDropLog(dropLog)))
    }
  }
}