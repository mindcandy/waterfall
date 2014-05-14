package com.mindcandy.waterfall.actor

import org.specs2.specification.After
import akka.testkit.TestKit
import org.specs2.time.NoTimeConversions
import akka.actor.ActorSystem
import org.specs2.SpecificationLike
import akka.testkit.TestProbe
import akka.actor.ActorRef
import akka.actor.Props
import scala.util.Success
import com.mindcandy.waterfall.actor.DropWorker.RunDrop
import com.mindcandy.waterfall.actor.DropSupervisor.JobResult


class DropWorkerSpec extends TestKit(ActorSystem("DropWorkerSpec")) with SpecificationLike with After with NoTimeConversions {
  override def is = s2"""
    DropWorker should
      run a drop and return success $runDrop
  """

  override def after: Any = TestKit.shutdownActorSystem(system)

  def runDrop = {
    val dropUID = "test1"
    val probe: TestProbe = TestProbe()
    val actor: ActorRef = system.actorOf(DropWorker.props)
    val request = RunDrop(dropUID, TestWaterfallDropFactory.getDropByUID(dropUID).get)

    probe.send(actor, request)
    probe.expectMsgClass(classOf[JobResult]).result must_== Success()
  }
}