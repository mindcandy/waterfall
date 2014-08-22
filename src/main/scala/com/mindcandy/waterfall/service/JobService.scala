package com.mindcandy.waterfall.service

import akka.actor.{ Actor, ActorRef, Props }
import argonaut.Argonaut._
import com.mindcandy.waterfall.actor.JobDatabaseManager._
import com.mindcandy.waterfall.actor.Protocol._
import com.mindcandy.waterfall.info.BuildInfo
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
          // list all jobs
          produce(instanceOf[DropJobList]) { completionFunction =>
            context =>
              jobDatabaseManager ! GetJobsForCompletion(completionFunction)
          }
        } ~
          post {
            // create or update a job
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
            // get information of a certain job
            produce(instanceOf[Option[DropJob]]) { completionFunction =>
              context =>
                jobDatabaseManager ! GetJobForCompletion(id, completionFunction)
            }
          }
        }
    } ~
    path("drops" / Segment) { dropUID =>
      get {
        // get jobs by dropUID
        println(dropUID)
        produce(instanceOf[DropJobList]) { completionFunction =>
          context =>
            jobDatabaseManager ! GetJobsWithDropUIDForCompletion(dropUID, completionFunction)
        }
      }
    } ~
    pathPrefix("logs") {
      anyParams('status.as[String].?, 'period.as[Int].?, 'jobID.as[Int].?) { (status, period, jobID) =>
        get {
          // list logs with optional filters
          val isException = status match {
            case Some("failure") => Some(true)
            case Some("success") => Some(false)
            case _ => None
          }
          produce(instanceOf[DropHistory]) { completionFunction =>
            context =>
              jobDatabaseManager ! GetLogsForCompletion(jobID, period, isException, completionFunction)
          }
        }
      }
    } ~
    path("schedule") {
      // list DropJob with enabled=true
      get {
        produce(instanceOf[DropJobList]) { completionFunction =>
          context =>
            jobDatabaseManager ! GetScheduleForCompletion(completionFunction)
        }
      }
    } ~
    path("status" / "info") {
      get {
        _.complete(
          ("version" := BuildInfo.version) ->:
          ("appName" := BuildInfo.name) ->:
          jEmptyObject
        )
      }
    }
    // format: ON
  }
}