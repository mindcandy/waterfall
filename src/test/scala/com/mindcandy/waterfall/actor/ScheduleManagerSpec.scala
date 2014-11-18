package com.mindcandy.waterfall.actor

import akka.actor.{ ActorRef, ActorSystem }
import akka.testkit.{ TestKit, TestProbe }
import com.github.nscala_time.time.Imports._
import com.mindcandy.waterfall.TestWaterfallDropFactory
import com.mindcandy.waterfall.actor.DropSupervisor.StartJob
import com.mindcandy.waterfall.actor.JobDatabaseManager.GetSchedule
import com.mindcandy.waterfall.actor.Protocol.{ DropJob, DropJobMap }
import com.mindcandy.waterfall.actor.ScheduleManager.CheckJobs
import com.typesafe.config.ConfigFactory
import org.specs2.SpecificationLike
import org.specs2.mock.Mockito
import org.specs2.specification.Step
import org.specs2.time.NoTimeConversions

import scala.concurrent.duration._
import scala.language.postfixOps

class ScheduleManagerSpec extends TestKit(ActorSystem("ScheduleManagerSpec", ConfigFactory.load()))
    with SpecificationLike
    with NoTimeConversions
    with Mockito {
  def is = s2"""
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
      schedule jobs correctly if they have identical dropUID $scheduleJobsWithIdenticalDropUID
      do not reschedule jobs  if the previous one has not ran yet $doNotRescheduleJobs
      do not schedule a job if it's cron is malformed $malformedCron
  """ ^ Step(afterAll)

  def afterAll = TestKit.shutdownActorSystem(system)

  def autoCheckJobs = {
    val databaseManager: TestProbe = TestProbe()
    val dropSupervisor: TestProbe = TestProbe()
    val actor: ActorRef = createScheduleActor(databaseManager, dropSupervisor, checkJobsPeriod = FiniteDuration(3, SECONDS))

    databaseManager.expectMsg(GetSchedule()) must not(throwA[AssertionError])
  }

  def checkJobs = {
    val probe: TestProbe = TestProbe()
    val databaseManager: TestProbe = TestProbe()
    val dropSupervisor: TestProbe = TestProbe()
    val actor: ActorRef = createScheduleActor(databaseManager, dropSupervisor)
    val request = CheckJobs()

    probe.send(actor, request)
    databaseManager.expectMsg(GetSchedule()) must not(throwA[AssertionError])
  }

  def scheduleOneJob = {
    val probe: TestProbe = TestProbe()
    val databaseManager: TestProbe = TestProbe()
    val dropSupervisor: TestProbe = TestProbe()
    val actor: ActorRef = createScheduleActor(databaseManager, dropSupervisor)
    val currentTime = DateTime.now + Period.seconds(5)

    val dropJob = createDropJob("EXRATE", "Exchange Rate", currentTime)
    val request = DropJobMap(Map(1 -> dropJob))

    probe.send(actor, request)
    dropSupervisor.expectMsg(StartJob(1, dropJob)) must not(throwA[AssertionError])
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
    val request = DropJobMap(Map(1 -> dropJob1, 2 -> dropJob2))

    probe.send(actor, request)
    dropSupervisor.expectMsgAllOf(StartJob(1, dropJob1), StartJob(2, dropJob2)) must not(throwA[AssertionError])
  }

  def cancelOneJob = {
    val probe: TestProbe = TestProbe()
    val databaseManager: TestProbe = TestProbe()
    val dropSupervisor: TestProbe = TestProbe()
    val actor: ActorRef = createScheduleActor(databaseManager, dropSupervisor)
    val currentTime = DateTime.now + Period.seconds(3)

    val dropJob = createDropJob("EXRATE", "Exchange Rate", currentTime)
    val request = DropJobMap(Map(1 -> dropJob))
    val cancelRequest = DropJobMap(Map())

    probe.send(actor, request)
    probe.send(actor, cancelRequest)
    dropSupervisor.expectNoMsg() must not(throwA[AssertionError])
  }

  def cancelOneJobAndKeepAnother = {
    val probe: TestProbe = TestProbe()
    val databaseManager: TestProbe = TestProbe()
    val dropSupervisor: TestProbe = TestProbe()
    val actor: ActorRef = createScheduleActor(databaseManager, dropSupervisor)
    val currentTime = DateTime.now + Period.seconds(3)

    val dropJob1 = createDropJob("EXRATE1", "Exchange Rate", currentTime)
    val dropJob2 = createDropJob("EXRATE2", "Exchange Rate", currentTime)
    val request = DropJobMap(Map(1 -> dropJob1, 2 -> dropJob2))
    val cancelRequest = DropJobMap(Map(2 -> dropJob2))

    probe.send(actor, request)
    probe.send(actor, cancelRequest)
    dropSupervisor.expectMsg(StartJob(2, dropJob2))
    dropSupervisor.expectNoMsg() must not(throwA[AssertionError])
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
    val request = DropJobMap(Map(1 -> dropJob1, 2 -> dropJob2))
    val cancelRequest = DropJobMap(Map(2 -> dropJob2, 3 -> dropJob3))

    probe.send(actor, request)
    probe.send(actor, cancelRequest)
    dropSupervisor.expectMsgAllOf(StartJob(2, dropJob2), StartJob(3, dropJob3))
    dropSupervisor.expectNoMsg() must not(throwA[AssertionError])
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
    val request = DropJobMap(Map(1 -> dropJob1, 2 -> dropJob2))

    probe.send(actor, request)
    dropSupervisor.expectMsg(StartJob(1, dropJob1))
    dropSupervisor.expectNoMsg() must not(throwA[AssertionError])
  }

  def scheduleOnlyWithCron = {
    val probe: TestProbe = TestProbe()
    val databaseManager: TestProbe = TestProbe()
    val dropSupervisor: TestProbe = TestProbe()
    val actor: ActorRef = createScheduleActor(databaseManager, dropSupervisor)
    val currentTime = DateTime.now + Period.seconds(3)

    val dropJob1 = createDropJob("EXRATE1", "Exchange Rate", currentTime)
    val dropJob2 = createDropJob("EXRATE2", "Exchange Rate", currentTime, isCron = false)
    val request = DropJobMap(Map(1 -> dropJob1, 2 -> dropJob2))

    probe.send(actor, request)
    dropSupervisor.expectMsg(StartJob(1, dropJob1))
    dropSupervisor.expectNoMsg() must not(throwA[AssertionError])
  }

  def scheduleJobsWithIdenticalDropUID = {
    val probe: TestProbe = TestProbe()
    val databaseManager: TestProbe = TestProbe()
    val dropSupervisor: TestProbe = TestProbe()
    val actor: ActorRef = createScheduleActor(databaseManager, dropSupervisor)

    val dropUID = "EXRATE"
    val currentTime = DateTime.now + Period.seconds(3)
    val dropJob1 = createDropJob(dropUID, "Exchange Rate", currentTime)
    val dropJob2 = createDropJob(dropUID, "Exchange Rate", currentTime)
    val request = DropJobMap(Map(1 -> dropJob1, 2 -> dropJob2))

    probe.send(actor, request)
    dropSupervisor.expectMsgAllOf(StartJob(1, dropJob1), StartJob(2, dropJob2)) must not(throwA[AssertionError])
  }

  def rescheduleJobs = {
    val probe: TestProbe = TestProbe()
    val databaseManager: TestProbe = TestProbe()
    val dropSupervisor: TestProbe = TestProbe()
    val actor: ActorRef = createScheduleActor(databaseManager, dropSupervisor)

    val currentTime = DateTime.now + Period.seconds(3)
    val dropJob = createDropJob("EXRATE", "Exchange Rate", currentTime)
    val request = DropJobMap(Map(1 -> dropJob))

    probe.send(actor, request)

    val newTime = DateTime.now + Period.seconds(3)
    val newDropJob = createDropJob("EXRATE", "Exchange Rate", newTime)
    val rescheduleRequest = DropJobMap(Map(2 -> newDropJob))

    probe.send(actor, rescheduleRequest)
    dropSupervisor.expectMsg(StartJob(2, newDropJob)) must not(throwA[AssertionError])
  }

  def doNotRescheduleJobs = {
    val probe: TestProbe = TestProbe()
    val databaseManager: TestProbe = TestProbe()
    val dropSupervisor: TestProbe = TestProbe()
    val actor: ActorRef = createScheduleActor(databaseManager, dropSupervisor)
    val currentTime = DateTime.now + Period.seconds(3)

    val dropJob = createDropJob("EXRATE", "Exchange Rate", currentTime)
    val request = DropJobMap(Map(1 -> dropJob))
    val rescheduleRequest = DropJobMap(Map(1 -> dropJob))

    probe.send(actor, request)
    probe.send(actor, rescheduleRequest)
    dropSupervisor.expectMsg(StartJob(1, dropJob))
    dropSupervisor.expectNoMsg() must not(throwA[AssertionError])
  }

  def malformedCron = {
    val probe: TestProbe = TestProbe()
    val databaseManager: TestProbe = TestProbe()
    val dropSupervisor: TestProbe = TestProbe()
    val actor: ActorRef = createScheduleActor(databaseManager, dropSupervisor)
    val dropJob = DropJob(Some(1), "EXRATE", "Exchange Rate", "desc", true, Option(s"malformed cron string"), TimeFrame.DAY_YESTERDAY, Map())
    val request = DropJobMap(Map(1 -> dropJob))

    probe.send(actor, request)
    dropSupervisor.expectNoMsg()
    databaseManager.expectNoMsg() must not(throwA[AssertionError])
  }

  def createScheduleActor(databaseManager: TestProbe, dropSupervisor: TestProbe, maxScheduleTime: FiniteDuration = FiniteDuration(1, MINUTES),
                          checkJobsPeriod: FiniteDuration = FiniteDuration(1, HOURS)): ActorRef =
    system.actorOf(ScheduleManager.props(databaseManager.ref, dropSupervisor.ref, new TestWaterfallDropFactory, maxScheduleTime, checkJobsPeriod))

  private def createDropJob(dropUid: String, name: String, currentTime: DateTime, isCron: Boolean = true): DropJob =
    DropJob(
      Option(1),
      dropUid,
      name,
      "desc",
      true, if (isCron) Option(s"${currentTime.secondOfMinute.getAsString} ${currentTime.minuteOfHour.getAsString} ${currentTime.hourOfDay.getAsString} * * ?") else None,
      TimeFrame.DAY_YESTERDAY,
      Map())
}