package com.mindcandy.waterfall.actor

import akka.actor._
import com.mindcandy.waterfall.WaterfallDrop
import com.mindcandy.waterfall.actor.DropSupervisor.JobResult
import com.mindcandy.waterfall.actor.Protocol.RunUID

object DropWorker extends ActorFactory {
  case class RunDrop[A <: AnyRef, B <: AnyRef](runUID: RunUID, waterfallDrop: WaterfallDrop[A, B])

  def props: Props = Props(new DropWorker())
}

class DropWorker extends Actor with ActorLogging {
  import com.mindcandy.waterfall.actor.DropWorker._

  def receive = {
    case RunDrop(runUID, dropJob) => {
      val result = dropJob.run
      sender ! JobResult(runUID, result)
      context.stop(self)
    }
  }
}