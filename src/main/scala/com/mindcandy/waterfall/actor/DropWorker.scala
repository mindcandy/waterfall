package com.mindcandy.waterfall.actor

import akka.actor._
import com.mindcandy.waterfall.WaterfallDrop
import com.mindcandy.waterfall.WaterfallDropFactory.DropUID
import scala.util.Try
import com.mindcandy.waterfall.IntermediateFormat
import com.mindcandy.waterfall.actor.DropSupervisor.JobResult
import com.mindcandy.waterfall.actor.DropSupervisor.JobResult

object DropWorker {
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

trait DropWorkerFactory {
  def createWorker(implicit context: ActorContext): ActorRef
}

object ContextDropWorkerFactory extends DropWorkerFactory {
  override def createWorker(implicit context: ActorContext): ActorRef = context.actorOf(DropWorker.props)
}