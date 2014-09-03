package com.mindcandy.waterfall.service

import akka.actor.{ ActorRef, ActorRefFactory }
import argonaut.Argonaut._
import com.mindcandy.waterfall.actor.JobDatabaseManager._
import com.mindcandy.waterfall.actor.LogStatus.LogStatus
import com.mindcandy.waterfall.actor.Protocol._
import com.mindcandy.waterfall.info.BuildInfo
import spray.routing.Route

case class JobServiceRoute(implicit val actorRefFactory: ActorRefFactory, jobDatabaseManager: ActorRef) extends ServiceRoute with ArgonautMarshallers {

  val route: Route = {
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
    path("drops" / Segment / "jobs") { dropUID =>
      get {
        // get jobs by dropUID
        produce(instanceOf[DropJobList]) { completionFunction =>
          context =>
            jobDatabaseManager ! GetJobsWithDropUIDForCompletion(dropUID, completionFunction)
        }
      }
    } ~
    pathPrefix("logs") {
      anyParams('status.as[LogStatus].?, 'period.as[Int].?, 'jobID.as[Int].?, 'dropUID.as[String].?) { (status, period, jobID, dropUID) =>
        get {
          produce(instanceOf[DropHistory]) { completionFunction =>
            context =>
              jobDatabaseManager ! GetLogsForCompletion(jobID, period, status, dropUID, completionFunction)
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