package com.mindcandy.waterfall.actor

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import com.github.nscala_time.time.Imports._
import com.mindcandy.waterfall.WaterfallDropFactory
import com.mindcandy.waterfall.actor.Protocol.{ DropJob, DropLog, JobID }
import com.mindcandy.waterfall.actor.TimeFrame._
import org.joda.time.Period
import org.joda.time.format.PeriodFormat

import scala.language.postfixOps
import scala.util.{ Failure, Success, Try }

object DropSupervisor {
  case class StartJob(jobID: JobID, job: DropJob)
  case class JobResult(jobID: JobID, result: Try[Unit])

  def calculateDate(timeFrame: TimeFrame) = timeFrame match {
    case DAY_TODAY => Some(DateTime.now)
    case DAY_YESTERDAY => Some(DateTime.now - 1.day)
    case DAY_TWO_DAYS_AGO => Some(DateTime.now - 2.days)
    case DAY_THREE_DAYS_AGO => Some(DateTime.now - 3.days)
  }

  def props(jobDatabaseManager: ActorRef, dropFactory: WaterfallDropFactory, dropWorkerFactory: ActorFactory = DropWorker): Props =
    Props(new DropSupervisor(jobDatabaseManager, dropFactory, dropWorkerFactory))
}

class DropSupervisor(val jobDatabaseManager: ActorRef, val dropFactory: WaterfallDropFactory, dropWorkerFactory: ActorFactory) extends Actor with ActorLogging {
  import com.mindcandy.waterfall.actor.DropSupervisor._

  private[this] var runningJobs = Map[JobID, (ActorRef, DateTime)]()

  def receive = {
    case StartJob(jobID, job) => runJob(jobID, job)
    case JobResult(jobID, result) => processResult(jobID, result)
  }

  def processResult(jobID: JobID, result: Try[Unit]) = {
    val endTime = DateTime.now
    runningJobs.get(jobID) match {
      case Some((worker, startTime)) => {
        val endTime = DateTime.now
        val runtime = PeriodFormat.getDefault().print(new Period((startTime to endTime)))
        result match {
          case Success(_) => {
            log.info(s"success for job $jobID after $runtime")
            jobDatabaseManager ! DropLog(None, jobID, startTime, Some(endTime), None, None)
          }
          case Failure(exception) => {
            log.error(s"failure for job $jobID after $runtime", exception)
            jobDatabaseManager !
              DropLog(None, jobID, startTime, Some(endTime), None, Some(s"${exception.toString}\n${exception.getStackTraceString}"))
          }
        }
        runningJobs -= jobID
      }
      case None => {
        val error = s"job result from job $jobID but not present in running jobs list"
        log.error(error)
        jobDatabaseManager ! DropLog(None, jobID, DateTime.now, Some(endTime), None, Some(error))
      }
    }
  }

  def runJob(jobID: JobID, job: DropJob) = runningJobs.get(jobID) match {
    case Some((actorRef, timestamp)) => {
      val error = s"job $jobID and drop uid ${job.dropUID} already running as actor $actorRef started at $timestamp"
      log.error(error)
      jobDatabaseManager ! DropLog(None, jobID, DateTime.now, Some(DateTime.now), None, Some(error))
    }
    case None => {
      val worker = dropWorkerFactory.createActor
      dropFactory.getDropByUID(job.dropUID, calculateDate(job.timeFrame), job.configuration) match {
        case Some(drop) => {
          val startTime = DateTime.now
          runningJobs += (jobID -> (worker, startTime))
          worker ! DropWorker.RunDrop(jobID, drop)
          jobDatabaseManager ! DropLog(None, jobID, startTime, None, None, None)
        }
        case None => {
          val error = s"factory has no drop for job $jobID and drop uid ${job.dropUID}"
          log.error(error)
          jobDatabaseManager ! DropLog(None, jobID, DateTime.now, Some(DateTime.now), None, Some(error))
        }
      }
    }
  }

  override def preStart() = {
    log.info(s"DropSupervisor starting with factory $dropFactory")
  }
}