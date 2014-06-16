package com.mindcandy.waterfall.actor

import com.mindcandy.waterfall.WaterfallDropFactory
import WaterfallDropFactory.DropUID
import akka.actor.Props
import scala.util.Try
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import com.mindcandy.waterfall.actor.Protocol.DropJob
import com.mindcandy.waterfall.actor.Protocol.DropLog
import com.github.nscala_time.time.Imports._
import scala.language.postfixOps
import org.joda.time.format.PeriodFormat
import org.joda.time.Period
import scala.util.Success
import scala.util.Failure
import TimeFrame._

object DropSupervisor {
  case class StartJob(job: DropJob)
  case class JobResult(jobUID: DropUID, result: Try[Unit])

  def props(jobDatabaseManager: ActorRef, dropFactory: WaterfallDropFactory, dropWorkerFactory: ActorFactory = DropWorker): Props =
    Props(new DropSupervisor(jobDatabaseManager, dropFactory, dropWorkerFactory))
}

class DropSupervisor(val jobDatabaseManager: ActorRef, val dropFactory: WaterfallDropFactory, dropWorkerFactory: ActorFactory) extends Actor with ActorLogging {
  import DropSupervisor._

  private[this] var runningJobs = Map[DropUID, (ActorRef, DateTime)]()

  def receive = {
    case StartJob(job) => runJob(job)
    case JobResult(jobUID, result) => processResult(jobUID, result)
  }

  def processResult(jobUID: DropUID, result: Try[Unit]) = {
    runningJobs.get(jobUID) match {
      case Some((worker, startTime)) => {
        val endTime = DateTime.now
        val runtime = PeriodFormat.getDefault().print(new Period((startTime to endTime)))
        result match {
          case Success(nothing) => {
            log.info(s"success for drop $jobUID after ${runtime}")
            jobDatabaseManager ! DropLog(jobUID, startTime, Some(endTime), None, None)
          }
          case Failure(exception) => {
            log.error(s"failure for drop $jobUID after ${runtime}", exception)
            jobDatabaseManager ! DropLog(jobUID, startTime, Some(endTime), None, Some(exception))
          }
        }
        runningJobs -= jobUID
      }
      case None => log.error(s"job result from job $jobUID but not present in running jobs list")
    }
  }

  def runJob(job: DropJob) = {
    val worker = dropWorkerFactory.createActor
    dropFactory.getDropByUID(job.dropUID, calculateDate(job.timeFrame), job.configuration) match {
      case Some(drop) => {
        val startTime = DateTime.now
        runningJobs += (job.dropUID -> (worker, startTime))
        worker ! DropWorker.RunDrop(job.dropUID, drop)
        jobDatabaseManager ! DropLog(job.dropUID, startTime, None, None, None)
      }
      case None => log.error(s"factory has no drop for ${job.dropUID}")
    }
  }

  def calculateDate(timeFrame: TimeFrame) = timeFrame match {
    case DAY_TODAY => Some(DateTime.now)
    case DAY_YESTERDAY => Some(DateTime.now - 1.day)
    case DAY_TWO_DAYS_AGO => Some(DateTime.now - 2.days)
    case DAY_THREE_DAYS_AGO => Some(DateTime.now - 3.days)
  }

  override def preStart() = {
    log.info(s"DropSupervisor starting with factory $dropFactory")
  }
}