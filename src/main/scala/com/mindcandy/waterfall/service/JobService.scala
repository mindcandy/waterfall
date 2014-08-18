package com.mindcandy.waterfall.service

import akka.actor.{ Actor, ActorRef, Props }
import argonaut.Argonaut._
import argonaut._
import com.mindcandy.waterfall.actor.JobDatabaseManager.{ GetJobForCompletion, GetScheduleForCompletion }
import com.mindcandy.waterfall.actor.Protocol._
import spray.routing.HttpService

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