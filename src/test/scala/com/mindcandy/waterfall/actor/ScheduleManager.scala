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

class ScheduleManagerSpec extends TestKit(ActorSystem("ScheduleManagerSpec")) with SpecificationLike with After with NoTimeConversions {
  override def is = s2"""
    ScheduleManager should
      should contact job database on check jobs message $checkJobs
      should schedule one job if one is send from the databaseManager $scheduleOneJob
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
}