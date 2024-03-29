package com.mindcandy.waterfall.actor

import akka.actor.{ Actor, ActorLogging, ActorRef, Cancellable, Props }
import com.github.nscala_time.time.Imports._
import com.mindcandy.waterfall.WaterfallDropFactory
import com.mindcandy.waterfall.actor.DropSupervisor.StartJob
import com.mindcandy.waterfall.actor.JobDatabaseManager.GetSchedule
import com.mindcandy.waterfall.actor.Protocol.Cron
import org.joda.time.chrono.ISOChronology
import org.quartz.CronExpression

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{ Failure, Success, Try }

object ScheduleManager {

  case class CheckJobs()

  def calculateNextFireTime(cron: Cron): Try[FiniteDuration] = Try {
    val cronExpression = new CronExpression(cron)
    val now = DateTime.now(ISOChronology.getInstanceUTC)
    val next = new DateTime(cronExpression.getNextValidTimeAfter(now.toDate))
    (now to next).millis millis
  }

  def props(jobDatabaseManager: ActorRef, dropSupervisor: ActorRef, dropFactory: WaterfallDropFactory, maxScheduleTime: FiniteDuration, checkJobsPeriod: FiniteDuration): Props =
    Props(new ScheduleManager(jobDatabaseManager, dropSupervisor, dropFactory, maxScheduleTime, checkJobsPeriod))
}

class ScheduleManager(val jobDatabaseManager: ActorRef, val dropSupervisor: ActorRef, val dropFactory: WaterfallDropFactory,
                      maxScheduleTime: FiniteDuration, checkJobsPeriod: FiniteDuration) extends Actor with ActorLogging {

  import com.mindcandy.waterfall.actor.Protocol._
  import com.mindcandy.waterfall.actor.ScheduleManager._

  private[this] var scheduledJobs = Map[JobID, (DropJob, Cancellable)]()

  // Schedule a periodic CheckJobs message to self
  context.system.scheduler.schedule(checkJobsPeriod, checkJobsPeriod, self, CheckJobs())(context.dispatcher)

  def receive = {
    case CheckJobs() => {
      log.debug("Received CheckJobs message")
      jobDatabaseManager ! GetSchedule()
    }
    case DropJobSchedule(jobs) => {
      log.debug(s"Received DropJobList($jobs)")

      for {
        jobID <- manageScheduledJobs(jobs.keySet)
        job = jobs(jobID)
        cron <- job.cron
        cancellable <- scheduleJob(jobID, job, cron)
      } yield {
        scheduledJobs += (jobID -> (job, cancellable))
      }
    }
    case startJob: StartJob => {
      scheduledJobs -= startJob.jobID
      dropSupervisor ! startJob
    }
  }

  def manageScheduledJobs(jobIDs: Set[JobID]): Set[JobID] = {
    val scheduledIDs = scheduledJobs.keySet & jobIDs
    for {
      removableJobID <- scheduledJobs.keySet &~ scheduledIDs
    } yield {
      val (_, cancellable) = scheduledJobs(removableJobID)
      cancellable.cancel
      scheduledJobs -= removableJobID
    }
    jobIDs &~ scheduledIDs
  }

  def scheduleJob(jobID: JobID, job: DropJob, cron: Cron): Option[Cancellable] = {
    calculateNextFireTime(cron) match {
      case Success(duration) if maxScheduleTime > duration =>
        log.debug(s"scheduled new job $job with duration $duration")
        Some(context.system.scheduler.scheduleOnce(duration, self, StartJob(jobID, job))(context.dispatcher))
      case Success(duration) =>
        log.debug(s"Job ${job.jobID} with drop uid ${job.dropUID} ignored, as it's scheduled to run after $duration and the current max schedule time is $maxScheduleTime")
        None
      case Failure(exception) => {
        log.error(s"could not resolve cron expression for ${job.jobID} with drop uid ${job.dropUID}", exception)
        None
      }
    }
  }
}