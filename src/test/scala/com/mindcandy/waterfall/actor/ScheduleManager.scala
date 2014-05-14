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

class ScheduleManagerSpec extends TestKit(ActorSystem("ScheduleManagerSpec")) with SpecificationLike with After with NoTimeConversions {
  override def is = s2"""
     ScheduleManager should
       should contact job database on check jobs message $checkJob
  """

  override def after: Any = TestKit.shutdownActorSystem(system)

  def checkJob = {
    val dropUID = "test1"
    val probe: TestProbe = TestProbe()
    val databaseManager: TestProbe = TestProbe()
    val dropSupervisor: TestProbe = TestProbe()
    val actor: ActorRef = system.actorOf(ScheduleManager.props(databaseManager.ref, dropSupervisor.ref, TestWaterfallDropFactory))
    val request = CheckJobs()

    probe.send(actor, request)
    databaseManager.expectMsgClass(classOf[GetSchedule]) must_== GetSchedule()
  }
}