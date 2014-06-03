package com.mindcandy.waterfall.service

import spray.routing.HttpService
import akka.actor.Actor
import argonaut._, Argonaut._
import akka.actor.Props
import akka.actor.ActorRef
import com.mindcandy.waterfall.actor.Protocol._
import com.mindcandy.waterfall.actor.JobDatabaseManager.GetJobForCompletion
import com.mindcandy.waterfall.actor.JobDatabaseManager.GetScheduleForCompletion

object JobServiceActor {
  def props(jobDatabaseManager: ActorRef): Props = Props(new JobServiceActor(jobDatabaseManager))
}

class JobServiceActor(val jobDatabaseManager: ActorRef) extends Actor with JobService {
  def actorRefFactory = context
  def receive = runRoute(route)
}

trait JobService extends HttpService with ArgonautMarshallers {
  def jobDatabaseManager: ActorRef

  val route = {
    path("jobs" / IntNumber) { id =>
      get {
        produce(instanceOf[Option[DropJob]]) { completionFunction =>
          context =>
            jobDatabaseManager ! GetJobForCompletion(id, completionFunction)
        }
      }
    } ~
      path("schedule") {
        get {
          produce(instanceOf[List[DropJob]]) { completionFunction =>
            context =>
              jobDatabaseManager ! GetScheduleForCompletion(completionFunction)
          }
        }
      }
  }
}