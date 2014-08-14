package com.mindcandy.waterfall.actor

import com.mindcandy.waterfall.WaterfallDropFactory
import akka.actor.Props
import scala.util.Try
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import com.mindcandy.waterfall.actor.Protocol.{ JobID, DropJob, DropLog }
import com.github.nscala_time.time.Imports._
import scala.language.postfixOps
import org.joda.time.format.PeriodFormat
import org.joda.time.Period
import scala.util.Success
import scala.util.Failure
import TimeFrame._

object DropSupervisor {
  case class StartJob(job: DropJob)
  case class JobResult(jobID: Int, result: Try[Unit])

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
  import DropSupervisor._

  private[this] var runningJobs = Map[JobID, (ActorRef, DateTime)]()

  def receive = {
    case StartJob(job) => runJob(job)
    case JobResult(jobID, result) => processResult(jobID, result)
  }

  def processResult(jobID: JobID, result: Try[Unit]) = {
    runningJobs.get(jobID) match {
      case Some((worker, startTime)) => {
        val endTime = DateTime.now
        val runtime = PeriodFormat.getDefault().print(new Period((startTime to endTime)))
        result match {
          case Success(nothing) => {
            val info = s"success for drop $jobID after ${runtime}"
            log.info(info)
            jobDatabaseManager ! DropLog(None, jobID, startTime, Some(endTime), Some(info), None)
          }
          case Failure(exception) => {
            val error = s"failure for drop $jobID after ${runtime}"
            log.error(error, exception)
            jobDatabaseManager !
              DropLog(
                None, jobID, startTime, Some(endTime), None,
                Some(s"${error}\n${exception.toString}\n${exception.getStackTraceString}"))
          }
        }
        runningJobs -= jobID
      }
      case None =>
        val error = s"job result from job $jobID but not present in running jobs list"
        log.error(error)
        jobDatabaseManager ! DropLog(None, jobID, DateTime.now, None, Some(error), None)
    }
  }

  def runJob(job: DropJob) = {
    job.jobID match {
      case None =>
        log.error(s"JobID is missing from DropJob: ${job.dropUID}")
      case Some(jobID) =>
        runningJobs.get(jobID) match {
          case Some((actorRef, timestamp)) =>
            val warning = s"job ${job.dropUID} already running as actor $actorRef started at $timestamp"
            log.warning(warning)
            jobDatabaseManager !
              DropLog(None, jobID, DateTime.now, None, Some(warning), None)
          case None => {
            val worker = dropWorkerFactory.createActor
            dropFactory.getDropByUID(job.dropUID, calculateDate(job.timeFrame), job.configuration) match {
              case Some(drop) =>
                val startTime = DateTime.now
                runningJobs += (jobID -> (worker, startTime))
                worker ! DropWorker.RunDrop(jobID, drop)
                jobDatabaseManager !
                  DropLog(None, jobID, startTime, None, Some(s"Job: ${job.dropUID} starts"), None)
              case None =>
                val error = s"factory has no drop for ${job.dropUID}"
                log.error(error)
                jobDatabaseManager !
                  DropLog(None, jobID, DateTime.now, None, Some(error), None)
            }
          }
        }
    }
  }

  override def preStart() = {
    log.info(s"DropSupervisor starting with factory $dropFactory")
  }
}