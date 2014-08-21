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
      pathEndOrSingleSlash {
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
      path(IntNumber) { id =>
        get {
          // get information of a certain DropJob
          produce(instanceOf[Option[DropJob]]) { completionFunction =>
            context =>
              jobDatabaseManager ! GetJobForCompletion(id, completionFunction)
          }
        }
      } ~
      path(Segment) { dropUID =>
        get {
          // get jobs by dropUID
          println(dropUID)
          produce(instanceOf[List[DropJob]]) { completionFunction =>
            context =>
              jobDatabaseManager ! GetJobsWithDropUIDForCompletion(dropUID, completionFunction)
          }
        }
      }
    } ~
    pathPrefix("logs") {
      pathEndOrSingleSlash {
        get {
          // list all logs
          produce(instanceOf[List[DropLog]]) { completionFunction =>
            context =>
              jobDatabaseManager ! GetLogsForCompletion(None, None, None, completionFunction)
          }
        } ~
        post {
          // insert a log to database, for testing purpose
          entity(as[DropLog]) { dropLog =>
            produce(instanceOf[Option[DropLog]]) { completionFunction =>
              context =>
                jobDatabaseManager ! PostLogForCompletion(dropLog, completionFunction)
            }
          }
        }
      } ~
      pathPrefix(Map("log" -> false, "error" -> true)) { isException =>
        pathEndOrSingleSlash {
          get {
            // list logs without/with exception field not null
            produce(instanceOf[List[DropLog]]) { completionFunction =>
              context =>
                jobDatabaseManager ! GetLogsForCompletion(None, None, Some(isException), completionFunction)
            }
          }
        } ~
        pathPrefix(IntNumber) { time =>
          pathEndOrSingleSlash {
            get {
              // further filter logs within not older than x hours
              produce(instanceOf[List[DropLog]]) { completionFunction =>
                context =>
                  jobDatabaseManager ! GetLogsForCompletion(None, Some(time), Some(isException), completionFunction)
              }
            }
          } ~
          path(IntNumber) { jobID =>
            get {
              // further filter logs by jobID
              produce(instanceOf[List[DropLog]]) { completionFunction =>
                context =>
                  jobDatabaseManager ! GetLogsForCompletion(Some(jobID), Some(time), Some(isException), completionFunction)
              }
            }
          }
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