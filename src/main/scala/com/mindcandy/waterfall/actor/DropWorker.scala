package com.mindcandy.waterfall.actor

import akka.actor._
import com.mindcandy.waterfall.WaterfallDrop
import com.mindcandy.waterfall.WaterfallDropFactory.DropUID
import scala.util.Try
import com.mindcandy.waterfall.IntermediateFormat
import com.mindcandy.waterfall.actor.DropSupervisor.JobResult
import com.mindcandy.waterfall.actor.DropSupervisor.JobResult

trait DropWorkerFactory {
  def createWorker(implicit context: ActorContext): ActorRef
}

object DropWorker extends DropWorkerFactory {
  case class RunDrop[A, B](dropUID: DropUID, waterfallDrop: WaterfallDrop[A, B])
  
  def props: Props = Props(new DropWorker())

  def createWorker(implicit context: ActorContext): ActorRef = context.actorOf(props)
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