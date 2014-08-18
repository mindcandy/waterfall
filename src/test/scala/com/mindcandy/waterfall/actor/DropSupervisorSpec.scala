package com.mindcandy.waterfall.actor

import akka.actor.ActorSystem
import akka.testkit.{ TestKit, TestProbe }
import com.github.nscala_time.time.Imports._
import com.mindcandy.waterfall.actor.DropSupervisor.{ JobResult, StartJob }
import com.mindcandy.waterfall.actor.DropWorker.RunDrop
import com.mindcandy.waterfall.actor.Protocol.{ DropJob, DropLog }
import com.mindcandy.waterfall.{ TestPassThroughWaterfallDrop, TestWaterfallDropFactory }
import org.specs2.SpecificationLike
import org.specs2.specification.After
import org.specs2.time.NoTimeConversions

import scala.concurrent.duration._
import scala.util.{ Failure, Success }

class DropSupervisorSpec extends TestKit(ActorSystem("DropSupervisorSpec"))
    with SpecificationLike
    with After
    with NoTimeConversions {
  override def is = s2"""
    DropSupervisor should
      run the job when it receives a start job message $runJobOnStartJob
      log the job when it receives a start job message $logJobOnStartJob
      log a success result when a job is completed successfully $logSuccess
      log a failure result when a job is completed unsuccessfully $logFailure
      do not run a job that is still running $doNotStartIfRunning
      rerun a job if previous run finished $reRunAfterFinished
      log to database if the drop not in factory $logToErrorIfNoFactoryDrop
      log to database if result not in running list $logToErrorIfResultNotInList
  """

  override def after: Any = TestKit.shutdownActorSystem(system)

  def runJobOnStartJob = {
    val probe = TestProbe()
    val jobDatabaseManager = TestProbe()
    val worker = TestProbe()
    val actor = system.actorOf(DropSupervisor.props(jobDatabaseManager.ref, new TestWaterfallDropFactory, TestDropWorkerFactory(worker.ref)))
    val request = createStartJob()

    probe.send(actor, request)
    val expectedMessage = RunDrop(1, TestPassThroughWaterfallDrop())
    worker.expectMsg(FiniteDuration(5, SECONDS), expectedMessage) must_== expectedMessage
  }

  def logJobOnStartJob = {
    val probe = TestProbe()
    val jobDatabaseManager = TestProbe()
    val actor = system.actorOf(DropSupervisor.props(jobDatabaseManager.ref, new TestWaterfallDropFactory))
    val request = createStartJob()

    probe.send(actor, request)
    jobDatabaseManager.expectMsgClass(FiniteDuration(5, SECONDS), classOf[DropLog]) match {
      case DropLog(None, 1, _, None, None, None) => success
      case _ => failure
    }
  }

  def logSuccess = {
    val probe = TestProbe()
    val jobDatabaseManager = TestProbe()
    val worker = TestProbe()
    val actor = system.actorOf(DropSupervisor.props(jobDatabaseManager.ref, new TestWaterfallDropFactory, TestDropWorkerFactory(worker.ref)))
    val request = createStartJob(TimeFrame.DAY_THREE_DAYS_AGO)
    val result = JobResult(1, Success(()))

    probe.send(actor, request)
    jobDatabaseManager.expectMsgClass(FiniteDuration(5, SECONDS), classOf[DropLog])

    probe.send(actor, result)
    jobDatabaseManager.expectMsgClass(FiniteDuration(5, SECONDS), classOf[DropLog]) match {
      case DropLog(None, 1, _, Some(_), None, None) => success
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
    val result = JobResult(1, Failure(exception))

    probe.send(actor, request)
    jobDatabaseManager.expectMsgClass(FiniteDuration(5, SECONDS), classOf[DropLog])

    probe.send(actor, result)
    jobDatabaseManager.expectMsgClass(FiniteDuration(5, SECONDS), classOf[DropLog]) match {
      case DropLog(None, 1, _, Some(_), None, Some(msg)) => success
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
    probe.send(actor, request)
    probe.send(actor, request)
    val expectedMessage = RunDrop(1, TestPassThroughWaterfallDrop())
    worker.expectMsg(FiniteDuration(5, SECONDS), expectedMessage) must_== expectedMessage
    worker.expectNoMsg(FiniteDuration(5, SECONDS)) must not(throwA[AssertionError])
  }

  def reRunAfterFinished = {
    val probe = TestProbe()
    val jobDatabaseManager = TestProbe()
    val worker = TestProbe()
    val actor = system.actorOf(DropSupervisor.props(jobDatabaseManager.ref, new TestWaterfallDropFactory, TestDropWorkerFactory(worker.ref)))
    val request = createStartJob(TimeFrame.DAY_TODAY)
    val expectedMessage = RunDrop(1, TestPassThroughWaterfallDrop())

    probe.send(actor, request)
    worker.expectMsg(FiniteDuration(5, SECONDS), expectedMessage) must_== expectedMessage

    probe.send(actor, JobResult(1, Success(Unit)))
    probe.send(actor, request)
    worker.expectMsg(FiniteDuration(5, SECONDS), expectedMessage) must_== expectedMessage
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
    val expectedMsg = Some(s"factory has no drop for ${dropUID}")
    jobDatabaseManager.expectMsgClass(classOf[DropLog]).exception must_== expectedMsg
  }

  def logToErrorIfResultNotInList = {
    val probe = TestProbe()
    val jobDatabaseManager = TestProbe()
    val worker = TestProbe()
    val actor = system.actorOf(DropSupervisor.props(jobDatabaseManager.ref, new TestWaterfallDropFactory, TestDropWorkerFactory(worker.ref)))

    probe.send(actor, createStartJob(TimeFrame.DAY_TODAY))
    jobDatabaseManager.expectMsgClass(classOf[DropLog])

    val jobID = 2
    probe.send(actor, JobResult(jobID, Success(())))

    val expectedMsg = Some(s"job result from job ${jobID} but not present in running jobs list")
    jobDatabaseManager.expectMsgClass(classOf[DropLog]).exception must_== expectedMsg
  }

  private def createStartJob(frame: TimeFrame.TimeFrame = TimeFrame.DAY_TODAY): StartJob = {
    val now = DateTime.now + Period.seconds(3)
    StartJob(1, DropJob(Some(1), "test1", "Exchange Rate", "description", true, s"${now.secondOfMinute.getAsString} ${now.minuteOfHour.getAsString} ${now.hourOfDay.getAsString} * * ?", frame, Map()))
  }
}
