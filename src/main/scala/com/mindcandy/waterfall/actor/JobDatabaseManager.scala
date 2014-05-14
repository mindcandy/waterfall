package com.mindcandy.waterfall.actor

import akka.actor.Actor
import akka.actor.Props
import Protocol.DropJob
import akka.actor.ActorLogging
import com.mindcandy.waterfall.actor.Protocol.DropJobList
import com.mindcandy.waterfall.actor.Protocol.DropLog

object JobDatabaseManager {
  case class GetJobForCompletion(jobId: Int, completionFunction: Option[DropJob] => Unit)
  case class GetScheduleForCompletion(completionFunction: List[DropJob] => Unit)
  case class GetSchedule()

  def props(): Props = Props(new JobDatabaseManager())
}

class JobDatabaseManager extends Actor with ActorLogging {
  import JobDatabaseManager._

  def receive = {
    case GetJobForCompletion(jobId, f) => {
      log.info(s"job lookup for id $jobId")
      val result = jobId match {
        case 1 => Some(DropJob("EXRATE", "Exchange Rate", true, "0 1 * * *"))
        case 3 => Some(DropJob("ADX", "Adx", true, "0 2 * * *"))
        case _ => None
      }
      f(result)
    }
    case GetScheduleForCompletion(f) => {
      log.info(s"schedule lookup for completion")
      f(List(DropJob("EXRATE", "Exchange Rate", true, "0 1 * * *"), DropJob("ADX", "Adx", true, "0 2 * * *")))
    }
    case GetSchedule() => {
      log.info(s"schedule lookup")
      sender ! DropJobList(List(DropJob("EXRATE", "Exchange Rate", true, "0 1 * * *"), DropJob("ADX", "Adx", true, "0 2 * * *")))
    }
    case dropLog: DropLog => {
      log.info(s"drop log received")
      log.info(dropLog.toString)
    }
  }
}