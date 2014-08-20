package com.mindcandy.waterfall.service

import akka.actor.{ Actor, ActorRef, Props }
import argonaut.Argonaut._
import com.mindcandy.waterfall.actor.JobDatabaseManager._
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
    // format: OFF
    pathPrefix("jobs") {
      get {
        // list all DropJob
        produce(instanceOf[List[DropJob]]) { completionFunction =>
          context =>
            jobDatabaseManager ! GetJobsForCompletion(completionFunction)
        }
      } ~
      post {
        // create or update a DropJob
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
      }
    } ~
    path("jobs" / Rest) { dropUID =>
      get {
        // get jobs by dropUID
        produce(instanceOf[List[DropJob]]) { completionFunction =>
          context =>
            jobDatabaseManager ! GetJobsWithDropUIDForCompletion(dropUID, completionFunction)
        }
      }
    } ~
    path("schedule") {
      // list DropJob with enabled=true
      get {
        produce(instanceOf[List[DropJob]]) { completionFunction =>
          context =>
            jobDatabaseManager ! GetScheduleForCompletion(completionFunction)
        }
      }
    }
    // format: ON
  }
}