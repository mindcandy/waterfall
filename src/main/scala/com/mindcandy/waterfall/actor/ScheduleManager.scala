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

  def calculateNextFireTime(cron: Option[String]): Try[Option[FiniteDuration]] = Try {
    cron.map { cronString =>
      val cronExpression = new CronExpression(cronString)
      val now = DateTime.now
      val next = new DateTime(cronExpression.getNextValidTimeAfter(now.toDate))
      (now to next).millis millis
    }
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
    case DropJobMap(jobs) => {
      log.debug(s"Received DropJobList($jobs)")
      // TODO: This is a bit clumsy now. Firstly map is unnecessary. And then
      // TODO: all these checks for cron.isDefined when it should be done once here
      // TODO: and then just pass the cron along.
      val newJobIDs = manageScheduledJobs(jobs)
      for {
        jobID <- newJobIDs
        cancellable <- scheduleJob(jobID, jobs(jobID))
      } yield {
        scheduledJobs += (jobID -> (jobs(jobID), cancellable))
      }
    }
    case startJob: StartJob => {
      scheduledJobs -= startJob.jobID
      dropSupervisor ! startJob
    }
  }

  def manageScheduledJobs(jobs: Map[JobID, DropJob]): Set[JobID] = {
    val jobIDs = jobs.filter(_._2.cron.isDefined).keySet
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

  def scheduleJob(jobID: JobID, job: DropJob): Option[Cancellable] = {
    calculateNextFireTime(job.cron) match {
      case Success(Some(duration)) if maxScheduleTime > duration =>
        log.debug(s"scheduled new job $job with duration $duration")
        Some(context.system.scheduler.scheduleOnce(duration, self, StartJob(jobID, job))(context.dispatcher))
      case Success(Some(duration)) =>
        log.debug(s"Job ${job.jobID} with drop uid ${job.dropUID} ignored, as it's scheduled to run after $duration and the current max schedule time is $maxScheduleTime")
        None
      case Success(None) =>
        log.debug(s"Job ${job.jobID} with drop uid ${job.dropUID} ignored, as it is a child")
        None
      case Failure(exception) => {
        log.error(s"could not resolve cron expression for ${job.jobID} with drop uid ${job.dropUID}", exception)
        None
      }
    }
  }
}