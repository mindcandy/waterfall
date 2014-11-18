package com.mindcandy.waterfall.actor

import java.util.UUID

import akka.actor.{ ActorRef, ActorSystem, Terminated }
import akka.testkit.{ TestKit, TestProbe }
import com.mindcandy.waterfall.TestWaterfallDropFactory
import com.mindcandy.waterfall.actor.DropSupervisor.JobResult
import com.mindcandy.waterfall.actor.DropWorker.RunDrop
import com.typesafe.config.ConfigFactory
import org.specs2.SpecificationLike
import org.specs2.specification.Step
import org.specs2.time.NoTimeConversions

import scala.concurrent.duration._

class DropWorkerSpec
    extends TestKit(ActorSystem("DropWorkerSpec", ConfigFactory.load()))
    with SpecificationLike
    with NoTimeConversions {
  def is = s2"""
    DropWorker should
      run a drop and return success $runDrop
      run a drop and return failure $runFailingDrop
      stop after running a job $stopActor
  """ ^ Step(afterAll)

  def afterAll = TestKit.shutdownActorSystem(system)

  def runDrop = {
    val runUID = UUID.randomUUID()
    val probe: TestProbe = TestProbe()
    val actor: ActorRef = system.actorOf(DropWorker.props)
    val request = RunDrop(1, runUID, new TestWaterfallDropFactory().getDropByUID("test1").get)

    probe.send(actor, request)
    probe.expectMsgClass(classOf[JobResult]).result must beSuccessfulTry
  }

  def runFailingDrop = {
    val runUID = UUID.randomUUID()
    val probe: TestProbe = TestProbe()
    val actor: ActorRef = system.actorOf(DropWorker.props)
    val request = RunDrop(1, runUID, new TestWaterfallDropFactory().getDropByUID("test2").get)

    probe.send(actor, request)
    probe.expectMsgClass(classOf[JobResult]).result must beFailedTry
  }

  def stopActor = {
    val runUID = UUID.randomUUID()
    val probe: TestProbe = TestProbe()
    val actor: ActorRef = system.actorOf(DropWorker.props)
    val request = RunDrop(1, runUID, new TestWaterfallDropFactory().getDropByUID("test1").get)
    probe.watch(actor)

    probe.send(actor, request)
    probe.expectMsgClass(classOf[JobResult])
    probe.expectTerminated(actor, FiniteDuration(5, SECONDS)) match {
      case Terminated(actor) => success
      case _ => failure
    }
  }
}