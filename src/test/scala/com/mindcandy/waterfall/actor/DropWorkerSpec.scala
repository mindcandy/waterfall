package com.mindcandy.waterfall.actor

import org.specs2.specification.After
import akka.testkit.TestKit
import org.specs2.time.NoTimeConversions
import akka.actor.{Terminated, ActorSystem, ActorRef, Props}
import org.specs2.SpecificationLike
import akka.testkit.TestProbe
import scala.util.Success
import com.mindcandy.waterfall.actor.DropWorker.RunDrop
import com.mindcandy.waterfall.actor.DropSupervisor.JobResult
import scala.concurrent.duration._
import com.mindcandy.waterfall.drop.TestWaterfallDropFactory


class DropWorkerSpec extends TestKit(ActorSystem("DropWorkerSpec")) with SpecificationLike with After with NoTimeConversions {
  override def is = s2"""
    DropWorker should
      run a drop and return success $runDrop
      stop after running a job $stopActor
  """

  override def after: Any = TestKit.shutdownActorSystem(system)

  def runDrop = {
    val dropUID = "test1"
    val probe: TestProbe = TestProbe()
    val actor: ActorRef = system.actorOf(DropWorker.props)
    val request = RunDrop(dropUID, new TestWaterfallDropFactory().getDropByUID(dropUID).get)

    probe.send(actor, request)
    probe.expectMsgClass(classOf[JobResult]).result must_== Success()
  }

  def stopActor = {
    val dropUID = "test1"
    val probe: TestProbe = TestProbe()
    val actor: ActorRef = system.actorOf(DropWorker.props)
    val request = RunDrop(dropUID, new TestWaterfallDropFactory().getDropByUID(dropUID).get)
    probe.watch(actor)

    probe.send(actor, request)
    probe.expectMsgClass(classOf[JobResult])
    probe.expectTerminated(actor, FiniteDuration(5, SECONDS)) match {
      case Terminated(actor) => success
      case _ => failure
    }
  }
}