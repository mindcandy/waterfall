package com.mindcandy.waterfall.actor

import akka.actor._
import com.mindcandy.waterfall.{WaterfallDropFactory, WaterfallDrop}
import WaterfallDropFactory.DropUID
import com.mindcandy.waterfall.actor.DropSupervisor.JobResult

object DropWorker extends ActorFactory {
  case class RunDrop[A, B](dropUID: DropUID, waterfallDrop: WaterfallDrop[A, B])
  
  def props: Props = Props(new DropWorker())
}

class DropWorker extends Actor with ActorLogging {
  import DropWorker._

  def receive = {
    case RunDrop(dropUID, dropJob) => {
      val result = dropJob.run
      sender ! JobResult(dropUID, result)
      context.stop(self)
    }
  }
}