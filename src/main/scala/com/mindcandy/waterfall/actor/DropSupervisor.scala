package com.mindcandy.waterfall.actor

import java.util.UUID

import akka.actor.{ Actor, ActorLogging, ActorRef, Props }
import com.github.nscala_time.time.Imports._
import com.mindcandy.waterfall.WaterfallDropFactory
import com.mindcandy.waterfall.actor.JobDatabaseManager._
import com.mindcandy.waterfall.actor.Protocol.{ DropJob, JobID, RunUID }
import com.mindcandy.waterfall.actor.TimeFrame._
import org.joda.time.Period
import org.joda.time.format.PeriodFormat

import scala.language.postfixOps
import scala.util.{ Failure, Success, Try }

object DropSupervisor {
  case class StartJob(jobID: JobID, job: DropJob)
  case class JobResult(runUID: RunUID, result: Try[Unit])
  case class RunJobImmediately(jobID: JobID, completionFunction: Option[DropJob] => Unit)

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

  private[this] var runningJobs = Map[RunUID, (ActorRef, DateTime, JobID)]()

  def receive = {
    case StartJob(jobID, job) => runJob(jobID, job)
    case JobResult(runUID, result) => processResult(runUID, result)
    case RunJobImmediately(jobID, f) => {
      log.debug(s"Got Run job:$jobID immediately request")
      jobDatabaseManager ! GetJobForCompletion(
        jobID,
        maybeJob => {
          maybeJob.map { job =>
            self ! StartJob(job.jobID.getOrElse(-1), job)
          }
          f(maybeJob)
        }
      )
    }
  }

  def processResult(runUID: RunUID, result: Try[Unit]) = {
    val endTime = DateTime.now
    runningJobs.get(runUID) match {
      case Some((worker, startTime, jobID)) => {
        val runtime = PeriodFormat.getDefault().print(new Period((startTime to endTime)))
        result match {
          case Success(_) => {
            log.info(s"success for run $runUID with job $jobID after $runtime")
            jobDatabaseManager ! FinishDropLog(runUID, endTime, None, None)
          }
          case Failure(exception) => {
            log.error(s"failure for run $runUID with job $jobID after $runtime", exception)
            jobDatabaseManager ! FinishDropLog(runUID, endTime, None, Some(exception))
          }
        }
        runningJobs -= runUID
      }
      case None => {
        val error = s"job result from runUID $runUID but not present in running jobs list"
        log.error(error)
        jobDatabaseManager ! FinishDropLog(runUID, endTime, None, Some(new IllegalArgumentException(error)))
      }
    }
  }

  def runJob(jobID: JobID, job: DropJob) = {
    val runUID = UUID.randomUUID()
    val startTime = DateTime.now
    (job.parallel, runningJobs.values.map(_._3).toSet.contains(jobID)) match {
      case (false, true) => {
        val error = s"job $jobID with drop uid ${job.dropUID} and name ${job.name} has already been running, run $runUID cancelled"
        log.error(error)
        jobDatabaseManager ! StartAndFinishDropLog(runUID, jobID, startTime, startTime, None, Some(new IllegalArgumentException(error)))
      }
      case (_, _) => {
        dropFactory.getDropByUID(job.dropUID, calculateDate(job.timeFrame), job.configuration) match {
          case Some(drop) => {
            val worker = dropWorkerFactory.createActor
            runningJobs += (runUID -> (worker, startTime, jobID))
            worker ! DropWorker.RunDrop(runUID, drop)
            jobDatabaseManager ! StartDropLog(runUID, jobID, startTime)
            log.info(s"starting run $runUID with job $jobID for dropUID ${job.dropUID} and name ${job.name}")
          }
          case None => {
            val error = s"factory has no drop for job $jobID with drop uid ${job.dropUID} and name ${job.name}"
            log.error(error)
            jobDatabaseManager ! StartAndFinishDropLog(runUID, jobID, startTime, startTime, None, Some(new IllegalArgumentException(error)))
          }
        }
      }
    }
  }

  override def preStart() = {
    log.info(s"DropSupervisor starting with factory $dropFactory")
  }
}