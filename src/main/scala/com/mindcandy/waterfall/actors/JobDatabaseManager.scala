package com.mindcandy.waterfall.actors

import akka.actor.Actor
import akka.actor.Props
import Protocol.DropJob
import akka.actor.ActorLogging
import com.mindcandy.waterfall.actors.Protocol.DropJobList

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
        case 1 => Some(DropJob(1, "Exchange Rate", true, "0 1 * * *", "EXRATE"))
        case 3 => Some(DropJob(3, "Adx", true, "0 2 * * *", "EXRATE"))
        case _ => None
      }
      f(result)
    }
    case GetScheduleForCompletion(f) => {
      log.info(s"schedule lookup for completion")
      f(List(DropJob(1, "Exchange Rate", true, "0 1 * * *", "EXRATE"), DropJob(3, "Adx", true, "0 2 * * *", "EXRATE")))
    }
    
    case GetSchedule() => {
      log.info(s"schedule lookup")
      sender ! DropJobList(List(DropJob(1, "Exchange Rate", true, "0 1 * * *", "EXRATE"), DropJob(3, "Adx", true, "0 2 * * *", "EXRATE")))
    }
  }
}