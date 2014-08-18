package com.mindcandy.waterfall.actor

import akka.actor.{ ActorRef, ActorSystem, Terminated }
import akka.testkit.{ TestKit, TestProbe }
import com.mindcandy.waterfall.TestWaterfallDropFactory
import com.mindcandy.waterfall.actor.DropSupervisor.JobResult
import com.mindcandy.waterfall.actor.DropWorker.RunDrop
import org.specs2.SpecificationLike
import org.specs2.specification.After
import org.specs2.time.NoTimeConversions

import scala.concurrent.duration._

class DropWorkerSpec extends TestKit(ActorSystem("DropWorkerSpec")) with SpecificationLike with After with NoTimeConversions {
  override def is = s2"""
    DropWorker should
      run a drop and return success $runDrop
      run a drop and return failure $runFailingDrop
      stop after running a job $stopActor
  """

  override def after: Any = TestKit.shutdownActorSystem(system)

  def runDrop = {
    val jobID = 1
    val probe: TestProbe = TestProbe()
    val actor: ActorRef = system.actorOf(DropWorker.props)
    val request = RunDrop(jobID, new TestWaterfallDropFactory().getDropByUID("test1").get)

    probe.send(actor, request)
    probe.expectMsgClass(classOf[JobResult]).result must beSuccessfulTry
  }

  def runFailingDrop = {
    val jobID = 2
    val probe: TestProbe = TestProbe()
    val actor: ActorRef = system.actorOf(DropWorker.props)
    val request = RunDrop(jobID, new TestWaterfallDropFactory().getDropByUID("test2").get)

    probe.send(actor, request)
    probe.expectMsgClass(classOf[JobResult]).result must beFailedTry
  }

  def stopActor = {
    val jobID = 1
    val probe: TestProbe = TestProbe()
    val actor: ActorRef = system.actorOf(DropWorker.props)
    val request = RunDrop(jobID, new TestWaterfallDropFactory().getDropByUID("test1").get)
    probe.watch(actor)

    probe.send(actor, request)
    probe.expectMsgClass(classOf[JobResult])
    probe.expectTerminated(actor, FiniteDuration(5, SECONDS)) match {
      case Terminated(actor) => success
      case _ => failure
    }
  }
}