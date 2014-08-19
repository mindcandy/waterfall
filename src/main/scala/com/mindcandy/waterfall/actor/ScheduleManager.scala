package com.mindcandy.waterfall.actor

import akka.actor.{ Actor, ActorLogging, ActorRef, Cancellable, Props }
import com.github.nscala_time.time.Imports._
import com.mindcandy.waterfall.WaterfallDropFactory
import com.mindcandy.waterfall.actor.DropSupervisor.StartJob
import com.mindcandy.waterfall.actor.JobDatabaseManager.GetSchedule
import org.quartz.CronExpression

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{ Failure, Success, Try }

object ScheduleManager {
  case class CheckJobs()

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
    case DropJobList(jobs) => {
      log.debug(s"Received DropJobList($jobs)")
      val newJobUIDs = manageScheduledJobs(jobs)
      for {
        (jobID, job) <- jobs
        if (newJobUIDs contains jobID)
        cancellable <- scheduleJob(jobID, job)
      } yield {
        scheduledJobs += (jobID -> (job, cancellable))
      }
    }
    case startJob: StartJob => {
      scheduledJobs -= startJob.jobID
      dropSupervisor ! startJob
    }
  }

  def manageScheduledJobs(jobs: Map[JobID, DropJob]): Set[JobID] = {
    val jobIDs = jobs.keySet
    val scheduledUIDs = scheduledJobs.keySet & jobIDs
    for {
      removableDropUID <- scheduledJobs.keySet &~ scheduledUIDs
    } yield {
      val (_, cancellable) = scheduledJobs(removableDropUID)
      cancellable.cancel
      scheduledJobs -= removableDropUID
    }
    jobIDs &~ scheduledUIDs
  }

  def scheduleJob(jobID: JobID, job: DropJob): Option[Cancellable] = {
    calculateNextFireTime(job.cron) match {
      case Success(duration) if maxScheduleTime > duration =>
        Some(context.system.scheduler.scheduleOnce(duration, self, StartJob(jobID, job))(context.dispatcher))
      case Success(duration) =>
        val debug = s"Job ${job.dropUID} ignored, as it's scheduled to run after $duration and the current max schedule time is $maxScheduleTime"
        log.debug(debug)
        jobDatabaseManager ! DropLog(None, job.jobID.get, DateTime.now, None, Some(debug), None)
        None
      case Failure(exception) => {
        log.debug("bad cron expression", exception)
        val error = "could not resolve cron expression:"
        log.error(error, exception)
        jobDatabaseManager !
          DropLog(
            None, job.jobID.get, DateTime.now, None, None,
            Some(s"${error}\n${exception.toString}\n${exception.getStackTraceString}"))
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