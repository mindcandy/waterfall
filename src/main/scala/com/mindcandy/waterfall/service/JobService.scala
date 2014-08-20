package com.mindcandy.waterfall.service

import akka.actor.{ Actor, ActorRef, Props }
import argonaut.Argonaut._
import com.mindcandy.waterfall.actor.JobDatabaseManager.{ UpdateJobForCompletion, PostJobForCompletion, GetJobForCompletion, GetScheduleForCompletion }
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
    path("jobs") {
      get {
        // list all DropJob
        produce(instanceOf[List[DropJob]]) { completionFunction =>
          context =>
            jobDatabaseManager ! GetScheduleForCompletion(completionFunction)
        }
      } ~
        post {
          // create a new DropJob
          entity(as[DropJob]) { dropJob =>
            produce(instanceOf[Option[DropJob]]) { completionFunction =>
              context =>
                jobDatabaseManager ! PostJobForCompletion(dropJob, completionFunction)
            }
          }
        }
    } ~
      path("jobs" / IntNumber) { id =>
        get {
          // get information of a certain DropJob
          produce(instanceOf[Option[DropJob]]) { completionFunction =>
            context =>
              jobDatabaseManager ! GetJobForCompletion(id, completionFunction)
          }
        } ~
          post {
            // update a certain DropJob
            entity(as[DropJob]) { dropJob =>
              produce(instanceOf[Option[DropJob]]) { completionFunction =>
                context =>
                  jobDatabaseManager ! UpdateJobForCompletion(id, dropJob, completionFunction)
              }
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