package com.mindcandy.waterfall.actor

import akka.actor._
import com.mindcandy.waterfall.{ WaterfallDropFactory, WaterfallDrop }
import com.mindcandy.waterfall.actor.DropSupervisor.JobResult

object DropWorker extends ActorFactory {
  case class RunDrop[A <: AnyRef, B <: AnyRef](jobID: Int, waterfallDrop: WaterfallDrop[A, B])

  def props: Props = Props(new DropWorker())
}

class DropWorker extends Actor with ActorLogging {
  import DropWorker._

  def receive = {
    case RunDrop(jobID, dropJob) => {
      val result = dropJob.run
      sender ! JobResult(jobID, result)
      context.stop(self)
    }
  }
}