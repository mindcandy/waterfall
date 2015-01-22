package com.mindcandy.waterfall.actor

import java.util.UUID

import akka.actor.{ Actor, ActorLogging, Props }
import com.mindcandy.waterfall.WaterfallDropFactory.DropUID
import com.mindcandy.waterfall.actor.LogStatus.LogStatus
import com.mindcandy.waterfall.actor.Protocol._
import com.mindcandy.waterfall.service.DB
import org.joda.time.DateTime

import scala.slick.jdbc.JdbcBackend.Database.dynamicSession

object JobDatabaseManager {
  case class GetJobForCompletion(jobId: JobID, completionFunction: Option[DropJob] => Unit)
  case class GetJobsForCompletion(completionFunction: DropJobList => Unit)
  case class GetJobsWithDropUIDForCompletion(dropUID: DropUID, completionFunction: DropJobList => Unit)
  case class GetChildrenWithJobIDForCompletion(parentJobId: JobID, completionFunction: DropJobList => Unit)
  case class GetScheduleForCompletion(completionFunction: DropJobList => Unit)
  case class GetSchedule()
  case class PostJobForCompletion(dropJob: DropJob, completionFunction: Option[DropJob] => Unit)
  case class GetLogsForCompletion(jobID: Option[JobID], time: Option[Int], status: Option[LogStatus], dropUID: Option[String], limit: Option[Int], offset: Option[Int], completionFunction: DropHistory => Unit)

  case class StartDropLog(runUID: UUID, jobID: Int, startTime: DateTime)
  case class FinishDropLog(runUID: UUID, endTime: DateTime, logOutput: Option[String], exception: Option[Throwable])
  case class StartAndFinishDropLog(runUID: UUID, jobID: Int, startTime: DateTime, endTime: DateTime, logOutput: Option[String], exception: Option[Throwable])

  def props(db: DB): Props = Props(new JobDatabaseManager(db))

  def convertException(exception: Option[Throwable]) = exception.map(ex => s"${ex.toString}\n${ex.getStackTraceString}")
}

class JobDatabaseManager(db: DB) extends Actor with ActorLogging {
  import com.mindcandy.waterfall.actor.JobDatabaseManager._

  import scala.slick.driver.JdbcDriver.simple._

  def getCronParent(jobId: JobID, jobs: List[DropJob]): Option[JobID] = {
    val jobOpt = jobs.find(_.jobID == Option(jobId))

    jobOpt match {
      case Some(job) => {
        job.parents.flatMap {
          _.headOption match {
            case Some(parentJobId) => getCronParent(parentJobId, jobs)
            case None => Option(jobId)
          }
        }.orElse(Option(jobId))
      }
      case None => {
        None
      }
    }
  }

  def receive = {
    case GetJobForCompletion(jobID, f) => {
      log.debug(s"job lookup for jobID:$jobID with completion")
      f(db.executeInSession(db.dropJobs.filter(_.jobID === jobID).firstOption))
    }
    case GetJobsForCompletion(f) => {
      log.debug(s"Get all jobs")
      val jobs = db.getJobsSorted()

      // Populate cron parent for display purposes
      val jobsWithCronParent = jobs.map(job => {
        job.copy(cronParent = getCronParent(job.jobID.get, jobs))
      })

      f(DropJobList(jobsWithCronParent))
    }
    case GetJobsWithDropUIDForCompletion(dropUID, f) => {
      log.debug(s"Get all jobs with dropUID: $dropUID")
      val jobs = db.executeInSession(db.dropJobsSorted.filter(_.dropUID === dropUID).list)
      f(DropJobList(jobs))
    }
    case GetChildrenWithJobIDForCompletion(parentJobID, f) => {
      log.debug(s"Get all child jobs for jobID:$parentJobID with completion")
      val jobs = db.getDropJobChildren(parentJobID)
      f(DropJobList(jobs))
    }
    case GetScheduleForCompletion(f) => {
      log.debug(s"schedule lookup for completion")
      val jobs = db.executeInSession(db.dropJobsSorted.filter(job => job.enabled && job.cron.?.isDefined).list)
      f(DropJobList(jobs))
    }
    case GetSchedule() => {
      log.debug(s"schedule lookup")
      val dropJobs = db.executeInSession(db.dropJobsSorted.filter(job => job.enabled && job.cron.?.isDefined).list)

      val jobs = (for {
        job <- dropJobs
        jobID <- job.jobID
      } yield (jobID, job)).toMap

      sender ! DropJobSchedule(jobs)
    }
    case StartDropLog(runUID, jobID, startTime) => {
      log.debug(s"received StartDropLog")
      db.insert(db.dropLogs, DropLog(runUID, jobID, startTime, None, None, None))
    }
    case FinishDropLog(runUID, endTime, logOutput, exception) => {
      log.debug(s"received StartDropLog")
      db.executeInSession(db.updateDropLog(runUID, endTime, logOutput, convertException(exception)))
    }
    case StartAndFinishDropLog(runUID, jobID, startTime, endTime, logOutput, exception) => {
      log.debug(s"received StartAndFinishDropLog")
      db.insert(db.dropLogs, DropLog(runUID, jobID, startTime, Option(endTime), logOutput, convertException(exception)))
    }
    case PostJobForCompletion(dropJob, f) => {
      log.debug(s"Insert or update a job $dropJob")
      f(db.db.withDynSession(db.insertOrUpdateDropJob(dropJob)))
    }
    case GetLogsForCompletion(jobID, time, status, dropUID, limit, offset, f) => {
      log.debug(s"Query logs for jobID:$jobID, time:$time, status:$status, dropUID:$dropUID, limit:$limit, offset:$offset")
      val logs = db.executeInSession(db.selectDropLog(jobID, time, status, dropUID, limit, offset))
      f(DropHistory(logs))
    }
  }
}