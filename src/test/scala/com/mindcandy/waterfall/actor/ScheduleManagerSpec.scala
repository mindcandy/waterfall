package com.mindcandy.waterfall.actor

import org.specs2.SpecificationLike
import akka.testkit.TestKit
import akka.actor.{ ActorSystem, ActorRef }
import org.specs2.specification.After
import org.specs2.time.NoTimeConversions
import akka.testkit.TestProbe
import com.mindcandy.waterfall.actor.JobDatabaseManager.GetSchedule
import com.mindcandy.waterfall.actor.Protocol.{ DropLog, DropJobList, DropJob }
import com.github.nscala_time.time.Imports._
import scala.language.postfixOps
import com.mindcandy.waterfall.actor.DropSupervisor.StartJob
import scala.concurrent.duration._
import org.joda.time
import com.mindcandy.waterfall.actor.ScheduleManager.CheckJobs
import com.mindcandy.waterfall.TestWaterfallDropFactory
import org.specs2.mock.Mockito

class ScheduleManagerSpec extends TestKit(ActorSystem("ScheduleManagerSpec"))
    with SpecificationLike
    with After
    with NoTimeConversions
    with Mockito {
  override def is = s2"""
    ScheduleManager should
      automatically schedule a CheckJobs message to itself $autoCheckJobs
      contact job database on check jobs message $checkJobs
      schedule one job if one is sent from the databaseManager $scheduleOneJob
      schedule two jobs if two are sent from the databaseManager $scheduleTwoJobsAtDifferentTimes
      not schedule a job if it is cancelled $cancelOneJob
      schedule jobs that are not cancelled even when others are $cancelOneJobAndKeepAnother
      schedule new jobs that are posted together with a cancellation request $scheduleNewJobAndCancelOther
      only schedule jobs that are supposed to be run within the next X time units $scheduleOnlyWithinTimeFrame
      reschedule jobs if they arrive after the previous one has ran $rescheduleJobs
      do not reschedule jobs  if the previous one has not ran yet $doNotRescheduleJobs
      do not schedule a job if it's cron is malformed $malformedCron
  """

  override def after: Any = TestKit.shutdownActorSystem(system)

  def autoCheckJobs = {
    val databaseManager: TestProbe = TestProbe()
    val dropSupervisor: TestProbe = TestProbe()
    val actor: ActorRef = createScheduleActor(databaseManager, dropSupervisor, checkJobsPeriod = FiniteDuration(3, SECONDS))

    databaseManager.expectMsgClass(FiniteDuration(5, SECONDS), classOf[GetSchedule]) must_== GetSchedule()
  }

  def checkJobs = {
    val probe: TestProbe = TestProbe()
    val databaseManager: TestProbe = TestProbe()
    val dropSupervisor: TestProbe = TestProbe()
    val actor: ActorRef = createScheduleActor(databaseManager, dropSupervisor)
    val request = CheckJobs()

    probe.send(actor, request)
    databaseManager.expectMsgClass(classOf[GetSchedule]) must_== GetSchedule()
  }

  def scheduleOneJob = {
    val probe: TestProbe = TestProbe()
    val databaseManager: TestProbe = TestProbe()
    val dropSupervisor: TestProbe = TestProbe()
    val actor: ActorRef = createScheduleActor(databaseManager, dropSupervisor)
    val currentTime = DateTime.now + Period.seconds(5)

    val dropJob = createDropJob("EXRATE", "Exchange Rate", currentTime)
    val request = DropJobList(List(dropJob))

    probe.send(actor, request)
    println("start job: " + StartJob(dropJob))
    dropSupervisor.expectMsgClass(FiniteDuration(10, SECONDS), classOf[StartJob]) must_== StartJob(dropJob)
  }

  def scheduleTwoJobsAtDifferentTimes = {
    val probe: TestProbe = TestProbe()
    val databaseManager: TestProbe = TestProbe()
    val dropSupervisor: TestProbe = TestProbe()
    val actor: ActorRef = createScheduleActor(databaseManager, dropSupervisor)
    val currentTime1 = DateTime.now + Period.seconds(3)
    val currentTime2 = DateTime.now + Period.seconds(6)

    val dropJob1 = createDropJob("EXRATE1", "Exchange Rate", currentTime1)
    val dropJob2 = createDropJob("EXRATE2", "Exchange Rate", currentTime2)
    val request = DropJobList(List(dropJob1, dropJob2))

    probe.send(actor, request)
    dropSupervisor.expectMsgClass(FiniteDuration(5, SECONDS), classOf[StartJob]) must_== StartJob(dropJob1)
    dropSupervisor.expectMsgClass(FiniteDuration(10, SECONDS), classOf[StartJob]) must_== StartJob(dropJob2)
  }

  def cancelOneJob = {
    val probe: TestProbe = TestProbe()
    val databaseManager: TestProbe = TestProbe()
    val dropSupervisor: TestProbe = TestProbe()
    val actor: ActorRef = createScheduleActor(databaseManager, dropSupervisor)
    val currentTime = DateTime.now + Period.seconds(3)

    val dropJob = createDropJob("EXRATE", "Exchange Rate", currentTime)
    val request = DropJobList(List(dropJob))
    val cancelRequest = DropJobList(List())

    probe.send(actor, request)
    probe.send(actor, cancelRequest)
    dropSupervisor.expectNoMsg(FiniteDuration(5, SECONDS)) must not(throwA[AssertionError])
  }

  def cancelOneJobAndKeepAnother = {
    val probe: TestProbe = TestProbe()
    val databaseManager: TestProbe = TestProbe()
    val dropSupervisor: TestProbe = TestProbe()
    val actor: ActorRef = createScheduleActor(databaseManager, dropSupervisor)
    val currentTime = DateTime.now + Period.seconds(3)

    val dropJob1 = createDropJob("EXRATE1", "Exchange Rate", currentTime)
    val dropJob2 = createDropJob("EXRATE2", "Exchange Rate", currentTime)
    val request = DropJobList(List(dropJob1, dropJob2))
    val cancelRequest = DropJobList(List(dropJob2))

    probe.send(actor, request)
    probe.send(actor, cancelRequest)
    dropSupervisor.expectMsgClass(FiniteDuration(5, SECONDS), classOf[StartJob]) must_== StartJob(dropJob2)
    dropSupervisor.expectNoMsg(FiniteDuration(5, SECONDS)) must not(throwA[AssertionError])
  }

  def scheduleNewJobAndCancelOther = {
    val probe: TestProbe = TestProbe()
    val databaseManager: TestProbe = TestProbe()
    val dropSupervisor: TestProbe = TestProbe()
    val actor: ActorRef = createScheduleActor(databaseManager, dropSupervisor)
    val currentTime = DateTime.now + Period.seconds(3)

    val dropJob1 = createDropJob("EXRATE1", "Exchange Rate", currentTime)
    val dropJob2 = createDropJob("EXRATE2", "Exchange Rate", currentTime)
    val dropJob3 = createDropJob("EXRATE3", "Exchange Rate", currentTime)
    val request = DropJobList(List(dropJob1, dropJob2))
    val cancelRequest = DropJobList(List(dropJob2, dropJob3))

    probe.send(actor, request)
    probe.send(actor, cancelRequest)
    dropSupervisor.expectMsgAllOf(FiniteDuration(10, SECONDS), StartJob(dropJob2), StartJob(dropJob3))
    dropSupervisor.expectNoMsg(FiniteDuration(5, SECONDS)) must not(throwA[AssertionError])
  }

  def scheduleOnlyWithinTimeFrame = {
    val probe: TestProbe = TestProbe()
    val databaseManager: TestProbe = TestProbe()
    val dropSupervisor: TestProbe = TestProbe()
    val actor: ActorRef = createScheduleActor(databaseManager, dropSupervisor, FiniteDuration(5, SECONDS))
    val currentTime1 = DateTime.now + Period.seconds(3)
    val currentTime2 = DateTime.now + Period.seconds(7)

    val dropJob1 = createDropJob("EXRATE1", "Exchange Rate", currentTime1)
    val dropJob2 = createDropJob("EXRATE2", "Exchange Rate", currentTime2)
    val request = DropJobList(List(dropJob1, dropJob2))

    probe.send(actor, request)
    dropSupervisor.expectMsgClass(FiniteDuration(5, SECONDS), classOf[StartJob]) must_== StartJob(dropJob1)
    dropSupervisor.expectNoMsg(FiniteDuration(10, SECONDS)) must not(throwA[AssertionError])
    databaseManager
      .expectMsgClass(classOf[DropLog])
      .logOutput
      .getOrElse("None") must startWith(s"Job ${dropJob2.dropUID} ignored")
  }

  def rescheduleJobs = {
    val probe: TestProbe = TestProbe()
    val databaseManager: TestProbe = TestProbe()
    val dropSupervisor: TestProbe = TestProbe()
    val actor: ActorRef = createScheduleActor(databaseManager, dropSupervisor)

    val currentTime = DateTime.now + Period.seconds(3)
    val dropJob = createDropJob("EXRATE", "Exchange Rate", currentTime)
    val request = DropJobList(List(dropJob))
    probe.send(actor, request)

    dropSupervisor.expectMsgClass(FiniteDuration(5, SECONDS), classOf[StartJob]) must_== StartJob(dropJob)

    val newTime = DateTime.now + Period.seconds(3)
    val newDropJob = createDropJob("EXRATE", "Exchange Rate", newTime)
    val rescheduleRequest = DropJobList(List(newDropJob))
    probe.send(actor, rescheduleRequest)

    dropSupervisor.expectMsgClass(FiniteDuration(5, SECONDS), classOf[StartJob]) must_== StartJob(newDropJob)
  }

  def doNotRescheduleJobs = {
    val probe: TestProbe = TestProbe()
    val databaseManager: TestProbe = TestProbe()
    val dropSupervisor: TestProbe = TestProbe()
    val actor: ActorRef = createScheduleActor(databaseManager, dropSupervisor)
    val currentTime = DateTime.now + Period.seconds(3)

    val dropJob = createDropJob("EXRATE", "Exchange Rate", currentTime)
    val request = DropJobList(List(dropJob))
    val rescheduleRequest = DropJobList(List(dropJob))

    probe.send(actor, request)
    probe.send(actor, rescheduleRequest)
    dropSupervisor.expectMsgClass(FiniteDuration(5, SECONDS), classOf[StartJob]) must_== StartJob(dropJob)
    dropSupervisor.expectNoMsg(FiniteDuration(5, SECONDS)) must not(throwA[AssertionError])
  }

  def malformedCron = {
    val probe: TestProbe = TestProbe()
    val databaseManager: TestProbe = TestProbe()
    val dropSupervisor: TestProbe = TestProbe()
    val actor: ActorRef = createScheduleActor(databaseManager, dropSupervisor)
    val dropJob = DropJob(Some(1), "EXRATE", "Exchange Rate", "desc", true, s"malformed cron string", TimeFrame.DAY_YESTERDAY, Map())
    val request = DropJobList(List(dropJob))

    probe.send(actor, request)
    dropSupervisor.expectNoMsg() must not(throwA[AssertionError])
    databaseManager
      .expectMsgClass(classOf[DropLog])
      .exception
      .getOrElse("None") must startWith("could not resolve cron expression:")
  }

  def createScheduleActor(databaseManager: TestProbe, dropSupervisor: TestProbe, maxScheduleTime: FiniteDuration = FiniteDuration(1, MINUTES),
                          checkJobsPeriod: FiniteDuration = FiniteDuration(1, HOURS)): ActorRef =
    system.actorOf(ScheduleManager.props(databaseManager.ref, dropSupervisor.ref, new TestWaterfallDropFactory, maxScheduleTime, checkJobsPeriod))

  private def createDropJob(dropUid: String, name: String, currentTime: time.DateTime): DropJob =
    DropJob(Some(1), dropUid, name, "desc", true, s"${currentTime.secondOfMinute.getAsString} ${currentTime.minuteOfHour.getAsString} ${currentTime.hourOfDay.getAsString} * * ?", TimeFrame.DAY_YESTERDAY, Map())
}