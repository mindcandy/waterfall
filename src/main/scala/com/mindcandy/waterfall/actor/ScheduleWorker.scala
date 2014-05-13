package com.mindcandy.waterfall.actor

import akka.actor.Props
import akka.actor.ActorRef
import akka.actor.ActorLogging
import akka.actor.Actor
import com.mindcandy.waterfall.actor.SchedulerManager.JobResult
import com.mindcandy.waterfall.WaterfallDrop
import com.mindcandy.waterfall.WaterfallDropFactory.DropUID
import scala.util.Try
import com.mindcandy.waterfall.IntermediateFormat

object ScheduleWorker {
  case class RunDrop[A, B](dropUID: DropUID, waterfallDrop: WaterfallDrop[A, B])
  
  def props: Props = Props(new ScheduleWorker())
}

class ScheduleWorker extends Actor with ActorLogging {
  import ScheduleWorker._

  def receive = {
    case RunDrop(dropUID, dropJob) => {
      val result = dropJob.run
      sender ! JobResult(dropUID, result)
    }
  }
}