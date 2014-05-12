package com.mindcandy.waterfall.actors

import akka.actor.Props
import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import com.mindcandy.waterfall.actors.JobDatabaseManager.GetSchedule
import com.mindcandy.waterfall.actors.Protocol.DropJobList
import com.mindcandy.waterfall.actors.Protocol.DropJob
import scala.util.Try
import scala.concurrent.duration._
import org.quartz.CronExpression
import com.github.nscala_time.time.Imports._
import scala.language.postfixOps
import akka.actor.Cancellable
import scala.util.Success
import scala.util.Failure
import com.mindcandy.waterfall.WaterfallDropFactory.DropUID
import org.joda.time.format.PeriodFormat
import com.mindcandy.waterfall.WaterfallDrop
import com.mindcandy.waterfall.WaterfallDropFactory

object SchedulerManager {
  case class CheckJobs()
  case class StartJob(jobUID: DropUID)
  case class JobResult(jobUID: DropUID, result: Try[Unit])

  def props(jobDatabaseManager: ActorRef, dropFactory: WaterfallDropFactory): Props = Props(new SchedulerManager(jobDatabaseManager, dropFactory))
}

class SchedulerManager(val jobDatabaseManager: ActorRef, dropFactory: WaterfallDropFactory) extends Actor with ActorLogging {
  import SchedulerManager._
  import Protocol._

  private[this] var scheduledJobs = Map[DropUID, (DropJob, Cancellable)]()
  private[this] var runningJobs = Map[DropUID, (ActorRef, DateTime)]()
  
  def receive = {
    case CheckJobs() => {
      jobDatabaseManager ! GetSchedule()
    }
    case DropJobList(jobs) => {
      val newJobUIDs = manageScheduledJobs(jobs.map(job => job.dropUID -> job).toMap)
      for {
        job <- jobs
        if (newJobUIDs contains job.dropUID)
        cancellable <- scheduleJob(job)
      } yield {
        scheduledJobs += (job.dropUID -> (job, cancellable))
      }
    }
    case StartJob(dropUID) => {
      val jobOpt = scheduledJobs.get(dropUID)
      scheduledJobs -= dropUID
      jobOpt match {
        case Some((job, cancellable)) => runJob(job)
        case None => log.error(s"trying to run a job for drop ${dropUID} that has been removed from scheduling but wasn't cancelled")
      }
    }
    case JobResult(jobUID, result) => {
      runningJobs.get(jobUID) match {
        case Some((actor, startTime)) => {
          val runtime = PeriodFormat.getDefault().print(new Period((startTime to DateTime.now)))
          result match {
            case Success(nothing) => log.info(s"success for drop $jobUID after ${runtime}")
            case Failure(exception) => log.error(s"failure for drop $jobUID after ${runtime}", exception)
          }
          runningJobs -= jobUID
        }
        case None => log.error(s"job result from job $jobUID but not present in running jobs list")
      }
    }
  }
  
  def manageScheduledJobs(jobs: Map[DropUID, DropJob]): Set[DropUID] = {
    val dropUIDs = jobs.keySet
    val scheduledUIDs = scheduledJobs.keySet & dropUIDs
    for {
      removableDropUID <- scheduledUIDs &~ dropUIDs
    } yield {
      val (job, cancellable) = scheduledJobs(removableDropUID)
      cancellable.cancel
      scheduledJobs -= removableDropUID
    }
    dropUIDs &~ scheduledUIDs
  }
  
  def scheduleJob(job: DropJob) : Option[Cancellable]= {
    calculateNextFireTime(job.cron) match {
      case Success(duration) => Some(context.system.scheduler.scheduleOnce(duration, self, StartJob(job.dropUID))(context.dispatcher))
      case Failure(exception) => {
        log.debug("bad cron expression", exception)
        log.error(s"could not resolve cron expression: ${exception.getMessage}")
        None
      }
    }
  }
  
  def calculateNextFireTime(cronString: String): Try[FiniteDuration] = Try {
    val cronExpression = new CronExpression(cronString)
    val now = DateTime.now
    val next = new DateTime(cronExpression.getNextValidTimeAfter(now.toDate))
    ((now to next).millis millis)
  }
  
  def runJob(job: DropJob) = {
    val worker = context.actorOf(ScheduleWorker.props(self))
    dropFactory.getDropByUID(job.dropUID) match {
      case Some(drop) => {
        runningJobs += (job.dropUID -> (worker, DateTime.now))
        worker ! ScheduleWorker.RunDrop(job.dropUID, drop)
      }
      case None => log.error(s"factory has no drop for ${job.dropUID}")
    }
  }
}