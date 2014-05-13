package com.mindcandy.waterfall.actor

import org.specs2.specification.After
import akka.testkit.TestKit
import org.specs2.time.NoTimeConversions
import akka.actor.ActorSystem
import org.specs2.SpecificationLike
import akka.testkit.TestProbe
import akka.actor.ActorRef
import akka.actor.Props
import com.mindcandy.waterfall.actor.ScheduleWorker.RunDrop
import com.mindcandy.waterfall.actor.SchedulerManager.JobResult
import scala.util.Success


class ScheduleWorkerSpec extends TestKit(ActorSystem("ScheduleWorkerSpec")) with SpecificationLike with After with NoTimeConversions {
  override def is = s2"""
     ScheduleWorker should
       run a drop and return success $runDrop
  """

  override def after: Any = TestKit.shutdownActorSystem(system)

  def runDrop = {
    val dropUID = "test1"
    val probe: TestProbe = TestProbe()
    val actor: ActorRef = system.actorOf(ScheduleWorker.props)
    val reqst = RunDrop(dropUID, TestWaterfallDropFactory.getDropByUID(dropUID).get)

    probe.send(actor, reqst)
    probe.expectMsgClass(classOf[JobResult]).result must_== Success()
  }
}