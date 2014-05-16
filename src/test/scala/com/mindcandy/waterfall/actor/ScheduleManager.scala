package com.mindcandy.waterfall.actor

import org.specs2.SpecificationLike
import akka.testkit.TestKit
import akka.actor.ActorSystem
import org.specs2.specification.After
import org.specs2.time.NoTimeConversions
import akka.testkit.TestProbe
import akka.actor.ActorRef
import scala.util.Success
import com.mindcandy.waterfall.actor.ScheduleManager.CheckJobs
import com.mindcandy.waterfall.actor.JobDatabaseManager.GetSchedule
import com.mindcandy.waterfall.actor.Protocol.DropJobList
import com.mindcandy.waterfall.actor.Protocol.DropJob
import com.github.nscala_time.time.Imports._
import scala.language.postfixOps
import com.mindcandy.waterfall.actor.DropSupervisor.StartJob
import scala.concurrent.duration._
import org.specs2.matcher.{Expectable, MatchSuccess}

class ScheduleManagerSpec extends TestKit(ActorSystem("ScheduleManagerSpec")) with SpecificationLike with After with NoTimeConversions  {
  override def is = s2"""
    ScheduleManager should
      contact job database on check jobs message $checkJobs
      schedule one job if one is sent from the databaseManager $scheduleOneJob
      schedule two jobs if two are sent from the databaseManager $scheduleTwoJobs
      not schedule a job if it is cancelled $cancelOneJob
      schedule jobs that are not cancelled even when others are $cancelOneJobAndKeepAnother
      schedule new jobs that are posted together with a cancellation request $scheduleNewJobAndCancelOther
  """

  override def after: Any = TestKit.shutdownActorSystem(system)

  def checkJobs = {
    val probe: TestProbe = TestProbe()
    val databaseManager: TestProbe = TestProbe()
    val dropSupervisor: TestProbe = TestProbe()
    val actor: ActorRef = system.actorOf(ScheduleManager.props(databaseManager.ref, dropSupervisor.ref, TestWaterfallDropFactory))
    val request = CheckJobs()

    probe.send(actor, request)
    databaseManager.expectMsgClass(classOf[GetSchedule]) must_== GetSchedule()
  }

  def scheduleOneJob = {
    val probe: TestProbe = TestProbe()
    val databaseManager: TestProbe = TestProbe()
    val dropSupervisor: TestProbe = TestProbe()
    val actor: ActorRef = system.actorOf(ScheduleManager.props(databaseManager.ref, dropSupervisor.ref, TestWaterfallDropFactory))
    val currentTime = DateTime.now + Period.seconds(5)
    
    val dropJob = DropJob("EXRATE", "Exchange Rate", true, s"${currentTime.secondOfMinute.getAsString} ${currentTime.minuteOfHour.getAsString} ${currentTime.hourOfDay.getAsString} * * ?")
    val request = DropJobList(List(dropJob))

    probe.send(actor, request)
    println("start job: " + StartJob(dropJob))
    dropSupervisor.expectMsgClass(FiniteDuration(10, SECONDS), classOf[StartJob]) must_== StartJob(dropJob)
  }

  def scheduleTwoJobs = {
    val probe: TestProbe = TestProbe()
    val databaseManager: TestProbe = TestProbe()
    val dropSupervisor: TestProbe = TestProbe()
    val actor: ActorRef = system.actorOf(ScheduleManager.props(databaseManager.ref, dropSupervisor.ref, TestWaterfallDropFactory))
    val currentTime = DateTime.now + Period.seconds(5)

    val dropJob1 = DropJob("EXRATE1", "Exchange Rate", true, s"${currentTime.secondOfMinute.getAsString} ${currentTime.minuteOfHour.getAsString} ${currentTime.hourOfDay.getAsString} * * ?")
    val dropJob2 = DropJob("EXRATE2", "Exchange Rate", true, s"${currentTime.secondOfMinute.getAsString} ${currentTime.minuteOfHour.getAsString} ${currentTime.hourOfDay.getAsString} * * ?")
    val request = DropJobList(List(dropJob1, dropJob2))

    probe.send(actor, request)
    dropSupervisor.expectMsgAllOf(FiniteDuration(10, SECONDS), StartJob(dropJob1), StartJob(dropJob2))
    success
  }

  def cancelOneJob = {
    val probe: TestProbe = TestProbe()
    val databaseManager: TestProbe = TestProbe()
    val dropSupervisor: TestProbe = TestProbe()
    val actor: ActorRef = system.actorOf(ScheduleManager.props(databaseManager.ref, dropSupervisor.ref, TestWaterfallDropFactory))
    val currentTime = DateTime.now + Period.seconds(3)

    val dropJob = DropJob("EXRATE", "Exchange Rate", true, s"${currentTime.secondOfMinute.getAsString} ${currentTime.minuteOfHour.getAsString} ${currentTime.hourOfDay.getAsString} * * ?")
    val request = DropJobList(List(dropJob))
    val cancelRequest = DropJobList(List())

    probe.send(actor, request)
    probe.send(actor, cancelRequest)
    dropSupervisor.expectNoMsg(FiniteDuration(5, SECONDS))
    success
  }

  def cancelOneJobAndKeepAnother = {
    val probe: TestProbe = TestProbe()
    val databaseManager: TestProbe = TestProbe()
    val dropSupervisor: TestProbe = TestProbe()
    val actor: ActorRef = system.actorOf(ScheduleManager.props(databaseManager.ref, dropSupervisor.ref, TestWaterfallDropFactory))
    val currentTime = DateTime.now + Period.seconds(3)

    val dropJob1 = DropJob("EXRATE1", "Exchange Rate", true, s"${currentTime.secondOfMinute.getAsString} ${currentTime.minuteOfHour.getAsString} ${currentTime.hourOfDay.getAsString} * * ?")
    val dropJob2 = DropJob("EXRATE2", "Exchange Rate", true, s"${currentTime.secondOfMinute.getAsString} ${currentTime.minuteOfHour.getAsString} ${currentTime.hourOfDay.getAsString} * * ?")
    val request = DropJobList(List(dropJob1, dropJob2))
    val cancelRequest = DropJobList(List(dropJob2))

    probe.send(actor, request)
    probe.send(actor, cancelRequest)
    dropSupervisor.expectMsgClass(FiniteDuration(10, SECONDS), classOf[StartJob]) must_== StartJob(dropJob2)
    dropSupervisor.expectNoMsg(FiniteDuration(5, SECONDS))
    success
  }

  def scheduleNewJobAndCancelOther = {
    val probe: TestProbe = TestProbe()
    val databaseManager: TestProbe = TestProbe()
    val dropSupervisor: TestProbe = TestProbe()
    val actor: ActorRef = system.actorOf(ScheduleManager.props(databaseManager.ref, dropSupervisor.ref, TestWaterfallDropFactory))
    val currentTime = DateTime.now + Period.seconds(3)

    val dropJob1 = DropJob("EXRATE1", "Exchange Rate", true, s"${currentTime.secondOfMinute.getAsString} ${currentTime.minuteOfHour.getAsString} ${currentTime.hourOfDay.getAsString} * * ?")
    val dropJob2 = DropJob("EXRATE2", "Exchange Rate", true, s"${currentTime.secondOfMinute.getAsString} ${currentTime.minuteOfHour.getAsString} ${currentTime.hourOfDay.getAsString} * * ?")
    val dropJob3 = DropJob("EXRATE3", "Exchange Rate", true, s"${currentTime.secondOfMinute.getAsString} ${currentTime.minuteOfHour.getAsString} ${currentTime.hourOfDay.getAsString} * * ?")
    val request = DropJobList(List(dropJob1, dropJob2))
    val cancelRequest = DropJobList(List(dropJob2, dropJob3))

    probe.send(actor, request)
    probe.send(actor, cancelRequest)
    dropSupervisor.expectMsgAllOf(FiniteDuration(10, SECONDS), StartJob(dropJob2), StartJob(dropJob3))
    dropSupervisor.expectNoMsg(FiniteDuration(5, SECONDS))
    success
  }

}