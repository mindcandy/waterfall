package com.mindcandy.waterfall.actor

import akka.actor.{ Actor, ActorLogging, Props }
import com.mindcandy.waterfall.WaterfallDropFactory.DropUID
import com.mindcandy.waterfall.actor.Protocol.{ DropJob, DropJobList, DropLog, JobID, dateTimeColumnType }
import com.github.nscala_time.time.Imports._

import scala.slick.jdbc.JdbcBackend.Database.dynamicSession

object JobDatabaseManager {
  case class GetJobForCompletion(jobId: JobID, completionFunction: Option[DropJob] => Unit)
  case class GetJobsForCompletion(completionFunction: List[DropJob] => Unit)
  case class GetJobsWithDropUIDForCompletion(dropUID: DropUID, completionFunction: List[DropJob] => Unit)
  case class GetScheduleForCompletion(completionFunction: List[DropJob] => Unit)
  case class GetSchedule()
  case class PostJobForCompletion(dropJob: DropJob, completionFunction: Option[DropJob] => Unit)
  case class GetLogsForCompletion(jobID: Option[JobID], withinHours: Option[Int], isException: Option[Boolean], completionFunction: List[DropLog] => Unit)
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
      val result = db.executeInSession {
        maybeExists(dropJob).fold(insertAndReturn(dropJob))(_ => updateAndReturn(dropJob))
      }
      f(result)
    }
    case GetLogsForCompletion(jobID, withinHours, isException, f) => {
      // TODO(deo.liang): use intersect when it's available in slick2.2
      log.debug(s"Quesry logs for jobID:${jobID.getOrElse(None)}, time:${withinHours.getOrElse(None)}, isException:${isException.getOrElse(None)}")
      val result = db.executeInSession {
        val resultFilterTime = withinHours.fold(db.dropLogs.sortBy(_.logID.desc)) { time =>
          val timeFrom = getCurrentTime - time.hour
          db.dropLogs
            .filter(x =>
            (x.endTime.isDefined && x.endTime >= timeFrom) ||
              (x.endTime.isEmpty && x.startTime >= timeFrom))
            .sortBy(_.logID.desc)
        }
        val resultFilterJobID = jobID.fold(resultFilterTime)(id => resultFilterTime.filter(_.jobID === id))
        isException match {
          case Some(true) => resultFilterJobID.filter(_.exception.isDefined).list
          case Some(false) => resultFilterJobID.filter(_.exception.isEmpty).list
          case None => resultFilterJobID.list
        }
      }
      f(result)
    }
    case PostLogForCompletion(dropLog, f) => {
      log.debug("Insert a log")
      val result = db.executeInSession(
        (db.dropLogs
          returning db.dropLogs.map(_.logID)
          into ((log, id) => Some(log.copy(logID = Some(id))))
        ) += dropLog
      )
      f(result)
    }
  }

  def maybeExists(dropJob: DropJob): Option[DropJob] =
    dropJob.jobID.flatMap(jid => db.dropJobs.filter(_.jobID === jid).firstOption)

  def insertAndReturn(dropJob: DropJob): Option[DropJob] = {
    log.debug("Insert a new job")
    (db.dropJobs returning db.dropJobs.map(_.jobID) into ((job, id) => Some(job.copy(jobID = Some(id))))) += dropJob
  }

  def updateAndReturn(dropJob: DropJob): Option[DropJob] = {
    log.debug("Update an existing job")
    val job = db.dropJobs.filter(_.jobID === dropJob.jobID)
    job.update(dropJob)
    job.firstOption
  }

  // for test injection
  def getCurrentTime = DateTime.now
}