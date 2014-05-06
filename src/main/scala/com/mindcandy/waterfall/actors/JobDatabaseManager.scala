package com.mindcandy.waterfall.actors

import akka.actor.Actor
import akka.actor.Props
import Protocol.DropJob
import akka.actor.ActorLogging

object JobDatabaseManager {
  case class GetJob(jobId: Int, completionFunction: Option[DropJob] => Unit)
  case class GetSchedule(completionFunction: List[DropJob] => Unit)

  def props(): Props = Props(new JobDatabaseManager())
}

class JobDatabaseManager extends Actor with ActorLogging {
  import JobDatabaseManager._

  def receive = {
    case GetJob(jobId, f) => {
      log.info(s"job lookup for id $jobId")
      val result = jobId match {
        case 1 => Some(DropJob(1, "Exchange Rate", true, "0 1 * * *", "EXRATE"))
        case 3 => Some(DropJob(3, "Adx", true, "0 2 * * *", "EXRATE"))
        case _ => None
      }
      f(result)
    }
    case GetSchedule(f) => {
      log.info(s"schedule lookup for id")
      f(List(DropJob(1, "Exchange Rate", true, "0 1 * * *", "EXRATE"), DropJob(3, "Adx", true, "0 2 * * *", "EXRATE")))
    }
  }
}