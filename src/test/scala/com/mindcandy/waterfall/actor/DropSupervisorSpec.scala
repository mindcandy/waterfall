package com.mindcandy.waterfall.actor

import java.util.UUID

import akka.actor.ActorSystem
import akka.testkit.{ TestKit, TestProbe }
import com.github.nscala_time.time.Imports._
import com.mindcandy.waterfall.actor.DropSupervisor.{ JobResult, StartJob }
import com.mindcandy.waterfall.actor.DropWorker.RunDrop
import com.mindcandy.waterfall.actor.JobDatabaseManager.{ FinishDropLog, StartAndFinishDropLog, StartDropLog }
import com.mindcandy.waterfall.actor.Protocol.DropJob
import com.mindcandy.waterfall.{ TestPassThroughWaterfallDrop, TestWaterfallDropFactory }
import com.typesafe.config.ConfigFactory
import org.specs2.SpecificationLike
import org.specs2.specification.Step
import org.specs2.time.NoTimeConversions

import scala.util.{ Failure, Success }

class DropSupervisorSpec extends TestKit(ActorSystem("DropSupervisorSpec", ConfigFactory.load()))
    with SpecificationLike
    with NoTimeConversions {
  def is = s2"""
    DropSupervisor should
      run the job when it receives a start job message $runJobOnStartJob
      log the job when it receives a start job message $logJobOnStartJob
      log a success result when a job is completed successfully $logSuccess
      log a failure result when a job is completed unsuccessfully $logFailure
      do not run a job that is still running $doNotStartIfRunning
      run job with identical jobID if parallel allowed $runJobsWithIdenticalJobIDIfParallelAllowed
      rerun a job if previous run finished $reRunAfterFinished
      log to database if the drop not in factory $logToErrorIfNoFactoryDrop
      log to database if result not in running list $logToErrorIfResultNotInList
  """ ^ Step(afterAll)

  def afterAll: Any = TestKit.shutdownActorSystem(system)

  def runJobOnStartJob = {
    val probe = TestProbe()
    val jobDatabaseManager = TestProbe()
    val worker = TestProbe()
    val actor = system.actorOf(DropSupervisor.props(jobDatabaseManager.ref, new TestWaterfallDropFactory, TestDropWorkerFactory(worker.ref)))
    val request = createStartJob()

    probe.send(actor, request)
    worker.expectMsgClass(classOf[RunDrop[_ <: AnyRef, _ <: AnyRef]]).waterfallDrop must_== TestPassThroughWaterfallDrop()
  }

  def logJobOnStartJob = {
    val probe = TestProbe()
    val jobDatabaseManager = TestProbe()
    val actor = system.actorOf(DropSupervisor.props(jobDatabaseManager.ref, new TestWaterfallDropFactory))
    val request = createStartJob()

    probe.send(actor, request)
    jobDatabaseManager.expectMsgClass(classOf[StartDropLog]).jobID must_== 1
  }

  def logSuccess = {
    val probe = TestProbe()
    val jobDatabaseManager = TestProbe()
    val worker = TestProbe()
    val actor = system.actorOf(DropSupervisor.props(jobDatabaseManager.ref, new TestWaterfallDropFactory, TestDropWorkerFactory(worker.ref)))
    val request = createStartJob(TimeFrame.DAY_THREE_DAYS_AGO)

    probe.send(actor, request)
    val runUID = jobDatabaseManager.expectMsgClass(classOf[StartDropLog]).runUID

    val result = JobResult(runUID, Success(()))
    probe.send(actor, result)
    jobDatabaseManager.expectMsgClass(classOf[FinishDropLog]) match {
      case FinishDropLog(`runUID`, _, None, None) => success
      case _ => failure
    }
  }

  def logFailure = {
    val probe = TestProbe()
    val jobDatabaseManager = TestProbe()
    val worker = TestProbe()
    val actor = system.actorOf(DropSupervisor.props(jobDatabaseManager.ref, new TestWaterfallDropFactory, TestDropWorkerFactory(worker.ref)))
    val request = createStartJob(TimeFrame.DAY_TWO_DAYS_AGO)
    val exception = new RuntimeException("test exception")

    probe.send(actor, request)
    val runUID = jobDatabaseManager.expectMsgClass(classOf[StartDropLog]).runUID

    val result = JobResult(runUID, Failure(exception))
    probe.send(actor, result)
    jobDatabaseManager.expectMsgClass(classOf[FinishDropLog]) match {
      case FinishDropLog(`runUID`, _, None, Some(msg)) => success
      case _ => failure
    }
  }

  def doNotStartIfRunning = {
    val probe = TestProbe()
    val jobDatabaseManager = TestProbe()
    val worker = TestProbe()
    val actor = system.actorOf(DropSupervisor.props(jobDatabaseManager.ref, new TestWaterfallDropFactory, TestDropWorkerFactory(worker.ref)))
    val request = createStartJob(TimeFrame.DAY_YESTERDAY)

    probe.send(actor, request)
    worker.expectMsgClass(classOf[RunDrop[_ <: AnyRef, _ <: AnyRef]])
    jobDatabaseManager.expectMsgClass(classOf[StartDropLog])
    probe.send(actor, request)
    jobDatabaseManager.expectMsgClass(classOf[StartAndFinishDropLog]) match {
      case StartAndFinishDropLog(runUID, 1, _, _, None, Some(exception)) => {
        val expectedMsg = s"job 1 with drop uid test1 and name Exchange Rate has already been running, run $runUID cancelled"
        exception.getMessage match {
          case `expectedMsg` => success
          case _ => failure
        }
      }
      case s: StartAndFinishDropLog => failure
    }
  }

  def runJobsWithIdenticalJobIDIfParallelAllowed = {
    val probe = TestProbe()
    val jobDatabaseManager = TestProbe()
    val worker = TestProbe()
    val actor = system.actorOf(DropSupervisor.props(jobDatabaseManager.ref, new TestWaterfallDropFactory, TestDropWorkerFactory(worker.ref), true))
    val request = createStartJob(TimeFrame.DAY_YESTERDAY)

    probe.send(actor, request)
    worker.expectMsgClass(classOf[RunDrop[_ <: AnyRef, _ <: AnyRef]])
    jobDatabaseManager.expectMsgClass(classOf[StartDropLog])
    probe.send(actor, request)
    worker.expectMsgClass(classOf[RunDrop[_ <: AnyRef, _ <: AnyRef]])
    jobDatabaseManager.expectMsgClass(classOf[StartDropLog]) must not(throwA[AssertionError])
  }

  def reRunAfterFinished = {
    val probe = TestProbe()
    val jobDatabaseManager = TestProbe()
    val worker = TestProbe()
    val actor = system.actorOf(DropSupervisor.props(jobDatabaseManager.ref, new TestWaterfallDropFactory, TestDropWorkerFactory(worker.ref)))
    val request = createStartJob(TimeFrame.DAY_TODAY)

    probe.send(actor, request)
    val runUID = worker.expectMsgClass(classOf[RunDrop[_ <: AnyRef, _ <: AnyRef]]).runUID

    probe.send(actor, JobResult(runUID, Success(Unit)))
    probe.send(actor, request)

    val newRunUID = worker.expectMsgClass(classOf[RunDrop[_ <: AnyRef, _ <: AnyRef]]).runUID
    runUID must_!= newRunUID
  }

  def logToErrorIfNoFactoryDrop = {
    val probe = TestProbe()
    val jobDatabaseManager = TestProbe()
    val worker = TestProbe()
    val actor = system.actorOf(DropSupervisor.props(jobDatabaseManager.ref, new TestWaterfallDropFactory, TestDropWorkerFactory(worker.ref)))
    val dropUID = "drop not in factory"
    val request = StartJob(
      1,
      DropJob(Some(1), dropUID, "", "", true, "", TimeFrame.DAY_TODAY, Map()))

    probe.send(actor, request)
    val expectedMsg = s"factory has no drop for job ${request.jobID} with drop uid ${request.job.dropUID} and name ${request.job.name}"
    jobDatabaseManager.expectMsgClass(classOf[StartAndFinishDropLog]) match {
      case StartAndFinishDropLog(_, 1, _, _, None, Some(exception)) => exception.getMessage match {
        case `expectedMsg` => success
        case _ => failure
      }
      case s: StartAndFinishDropLog => failure
    }
  }

  def logToErrorIfResultNotInList = {
    val probe = TestProbe()
    val jobDatabaseManager = TestProbe()
    val worker = TestProbe()
    val actor = system.actorOf(DropSupervisor.props(jobDatabaseManager.ref, new TestWaterfallDropFactory, TestDropWorkerFactory(worker.ref)))

    probe.send(actor, createStartJob(TimeFrame.DAY_TODAY))
    jobDatabaseManager.expectMsgClass(classOf[StartDropLog])

    val runUID = UUID.randomUUID()
    probe.send(actor, JobResult(runUID, Success(())))

    val expectedMsg = s"job result from runUID ${runUID} but not present in running jobs list"
    jobDatabaseManager.expectMsgClass(classOf[FinishDropLog]) match {
      case FinishDropLog(`runUID`, _, None, Some(exception)) => exception.getMessage match {
        case `expectedMsg` => success
        case _ => failure
      }
      case _ => failure
    }
  }

  private def createStartJob(frame: TimeFrame.TimeFrame = TimeFrame.DAY_TODAY): StartJob = {
    val now = DateTime.now + Period.seconds(3)
    StartJob(1, DropJob(Some(1), "test1", "Exchange Rate", "description", true, s"${now.secondOfMinute.getAsString} ${now.minuteOfHour.getAsString} ${now.hourOfDay.getAsString} * * ?", frame, Map()))
  }
}
