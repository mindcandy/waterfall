package com.mindcandy.waterfall.actor

import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import com.mindcandy.waterfall.actor.JobDatabaseManager.GetSchedule
import scala.util.Try
import scala.concurrent.duration._
import org.quartz.CronExpression
import com.github.nscala_time.time.Imports._
import scala.language.postfixOps
import akka.actor.Cancellable
import scala.util.Success
import scala.util.Failure
import com.mindcandy.waterfall.WaterfallDropFactory
import WaterfallDropFactory.DropUID
import com.mindcandy.waterfall.actor.DropSupervisor.StartJob
import com.mindcandy.waterfall.WaterfallDropFactory

object ScheduleManager {
  case class CheckJobs()

  def props(jobDatabaseManager: ActorRef, dropSupervisor: ActorRef, dropFactory: WaterfallDropFactory, maxScheduleTime: FiniteDuration, checkJobsPeriod: FiniteDuration): Props =
    Props(new ScheduleManager(jobDatabaseManager, dropSupervisor, dropFactory, maxScheduleTime, checkJobsPeriod))
}

class ScheduleManager(val jobDatabaseManager: ActorRef, val dropSupervisor: ActorRef, val dropFactory: WaterfallDropFactory,
                      maxScheduleTime: FiniteDuration, checkJobsPeriod: FiniteDuration) extends Actor with ActorLogging {
  import ScheduleManager._
  import Protocol._

  private[this] var scheduledJobs = Map[DropUID, (DropJob, Cancellable)]()

  // Schedule a periodic CheckJobs message to self
  context.system.scheduler.schedule(checkJobsPeriod, checkJobsPeriod, self, CheckJobs())(context.dispatcher)

  def receive = {
    case CheckJobs() => {
      log.debug("Received CheckJobs message")
      jobDatabaseManager ! GetSchedule()
    }
    case DropJobList(jobs) => {
      log.debug(s"Received DropJobList($jobs)")
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
      removableDropUID <- scheduledJobs.keySet &~ scheduledUIDs
    } yield {
      val (job, cancellable) = scheduledJobs(removableDropUID)
      cancellable.cancel
      scheduledJobs -= removableDropUID
    }
    dropUIDs &~ scheduledUIDs
  }

  def scheduleJob(job: DropJob) : Option[Cancellable]= {
    calculateNextFireTime(job.cron) match {
      case Success(duration) if maxScheduleTime > duration =>
        Some(context.system.scheduler.scheduleOnce(duration, dropSupervisor, StartJob(job))(context.dispatcher))
      case Success(duration) =>
        log.debug(s"Job $job ignored, as it's scheduled to run after $duration and the current max schedule time is $maxScheduleTime")
        None
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