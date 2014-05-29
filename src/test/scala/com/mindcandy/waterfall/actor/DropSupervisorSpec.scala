package com.mindcandy.waterfall.actor

import akka.testkit.{TestProbe, TestKit}
import akka.actor.{ActorContext, ActorRef, ActorSystem}
import org.specs2.SpecificationLike
import org.specs2.specification.After
import org.specs2.time.NoTimeConversions
import org.joda.time
import com.github.nscala_time.time.Imports._
import com.mindcandy.waterfall.actor.DropSupervisor.{JobResult, StartJob}
import com.mindcandy.waterfall.actor.Protocol.{DropLog, DropJob}
import com.mindcandy.waterfall.actor.DropWorker.RunDrop
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}
import com.mindcandy.waterfall.drop.{TestWaterfallDropFactory, TestPassThroughWaterfallDrop}

class DropSupervisorSpec extends TestKit(ActorSystem("DropSupervisorSpec")) with SpecificationLike with After with NoTimeConversions  {
  override def is = s2"""
    DropSupervisor should
      run the job when it receives a start job message $runJobOnStartJob
      log the job when it receives a start job message $logJobOnStartJob
      log a success result when a job is completed successfully $logSuccess
      log a failure result when a job is completed unsuccessfully $logFailure
  """

  override def after: Any = TestKit.shutdownActorSystem(system)

  def runJobOnStartJob = {
    val probe = TestProbe()
    val jobDatabaseManager = TestProbe()
    val worker = TestProbe()
    val actor = system.actorOf(DropSupervisor.props(jobDatabaseManager.ref, new TestWaterfallDropFactory, TestDropWorkerFactory(worker.ref)))
    val currentTime = DateTime.now + Period.seconds(3)
    val request = createStartJob("test1", "Exchange Rate", currentTime)

    probe.send(actor, request)
    val expectedMessage = RunDrop("test1", TestPassThroughWaterfallDrop())
    worker.expectMsg(FiniteDuration(5, SECONDS), expectedMessage) must_== expectedMessage
  }

  def logJobOnStartJob = {
    val probe = TestProbe()
    val jobDatabaseManager = TestProbe()
    val actor = system.actorOf(DropSupervisor.props(jobDatabaseManager.ref, new TestWaterfallDropFactory))
    val currentTime = DateTime.now + Period.seconds(3)
    val request = createStartJob("test1", "Exchange Rate", currentTime)

    probe.send(actor, request)
    jobDatabaseManager.expectMsgClass(FiniteDuration(5, SECONDS), classOf[DropLog]) match {
      case DropLog("test1", _, None, None, None) => success
      case _ => failure
    }
  }

  def logSuccess = {
    val probe = TestProbe()
    val jobDatabaseManager = TestProbe()
    val worker = TestProbe()
    val actor = system.actorOf(DropSupervisor.props(jobDatabaseManager.ref, new TestWaterfallDropFactory, TestDropWorkerFactory(worker.ref)))
    val currentTime = DateTime.now + Period.seconds(3)
    val request = createStartJob("test1", "Exchange Rate", currentTime)
    val result = JobResult("test1", Success(()))

    probe.send(actor, request)
    jobDatabaseManager.expectMsgClass(FiniteDuration(5, SECONDS), classOf[DropLog])

    probe.send(actor, result)
    jobDatabaseManager.expectMsgClass(FiniteDuration(5, SECONDS), classOf[DropLog]) match {
      case DropLog("test1", _, Some(endTime), None, None) => success
      case _ => failure
    }
  }

  def logFailure = {
    val probe = TestProbe()
    val jobDatabaseManager = TestProbe()
    val worker = TestProbe()
    val actor = system.actorOf(DropSupervisor.props(jobDatabaseManager.ref, new TestWaterfallDropFactory, TestDropWorkerFactory(worker.ref)))
    val currentTime = DateTime.now + Period.seconds(3)
    val request = createStartJob("test1", "Exchange Rate", currentTime)
    val exception = new RuntimeException("test exception")
    val result = JobResult("test1", Failure(exception))

    probe.send(actor, request)
    jobDatabaseManager.expectMsgClass(FiniteDuration(5, SECONDS), classOf[DropLog])

    probe.send(actor, result)
    jobDatabaseManager.expectMsgClass(FiniteDuration(5, SECONDS), classOf[DropLog]) match {
      case DropLog("test1", _, Some(endTime), None, Some(`exception`)) => success
      case _ => failure
    }
  }

  private def createStartJob(dropUid: String, name: String, currentTime: time.DateTime): StartJob =
    StartJob(DropJob(dropUid, name, true, s"${currentTime.secondOfMinute.getAsString} ${currentTime.minuteOfHour.getAsString} ${currentTime.hourOfDay.getAsString} * * ?"))
}
