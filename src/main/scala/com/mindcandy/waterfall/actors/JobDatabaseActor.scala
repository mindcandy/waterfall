package com.mindcandy.waterfall.actors

import akka.actor.Actor
import akka.actor.Props
import Protocol.DropJob
import akka.actor.ActorLogging

object JobDatabaseActor {
  case class GetJob(jobId: Int, completionFunction: Option[DropJob] => Unit)

  def props(): Props = Props(new JobDatabaseActor())
}

class JobDatabaseActor extends Actor with ActorLogging {
  import JobDatabaseActor._

  def receive = {
    case GetJob(jobId, f) => {
      log.info(s"job lookup for id $jobId")
      val result = jobId match {
        case 1 => Some(DropJob(1, "Exchange Rate", "0 1 * * *", "EXRATE"))
        case 3 => Some(DropJob(3, "Adx", "0 1 * * *", "EXRATE"))
        case _ => None
      }
      f(result)
    }
  }
}