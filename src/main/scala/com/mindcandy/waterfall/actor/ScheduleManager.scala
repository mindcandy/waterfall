package com.mindcandy.waterfall.actor

import akka.actor.Props
import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import com.mindcandy.waterfall.actor.JobDatabaseManager.GetSchedule
import com.mindcandy.waterfall.actor.Protocol.DropJobList
import com.mindcandy.waterfall.actor.Protocol.DropJob
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
import com.mindcandy.waterfall.actor.DropSupervisor.StartJob

object ScheduleManager {
  case class CheckJobs()

  def props(jobDatabaseManager: ActorRef, dropSupervisor: ActorRef, dropFactory: WaterfallDropFactory): Props = 
    Props(new ScheduleManager(jobDatabaseManager, dropSupervisor, dropFactory))
}

class ScheduleManager(val jobDatabaseManager: ActorRef, val dropSupervisor: ActorRef, val dropFactory: WaterfallDropFactory) extends Actor with ActorLogging {
  import ScheduleManager._
  import Protocol._

  private[this] var scheduledJobs = Map[DropUID, (DropJob, Cancellable)]()

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
      case Success(duration) => Some(context.system.scheduler.scheduleOnce(duration, dropSupervisor, StartJob(job))(context.dispatcher))
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
}