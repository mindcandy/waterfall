package com.mindcandy.waterfall.service

import akka.actor.{ ActorRef, ActorRefFactory }
import argonaut.Argonaut._
import com.mindcandy.waterfall.actor.DropSupervisor.RunJobImmediately
import com.mindcandy.waterfall.actor.JobDatabaseManager._
import com.mindcandy.waterfall.actor.LogStatus.LogStatus
import com.mindcandy.waterfall.actor.Protocol._
import com.mindcandy.waterfall.info.BuildInfo
import spray.routing.Route

case class JobServiceRoute(jobDatabaseManager: ActorRef, dropSupervisor: ActorRef)(implicit val actorRefFactory: ActorRefFactory) extends ServiceRoute with ArgonautMarshallers {

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
          entity(as[DropJobModel]) { dropJobModel =>
            validate((dropJobModel.cron, dropJobModel.parents) match {
              case (Some(_), None) | (None, Some(_)) => true
              case _ => false
            }, "A job can only have a cron or parents") {
              produce(instanceOf[Option[DropJob]]) { completionFunction =>
                context =>
                  jobDatabaseManager ! PostJobForCompletion(dropJobModel.asJob, dropJobModel.parents, completionFunction)
              }
            }
          }
        }
      } ~
      pathPrefix(IntNumber) { id =>
        pathEndOrSingleSlash {
          get {
            // get information of a certain job
            produce(instanceOf[Option[DropJob]]) { completionFunction =>
              context =>
                jobDatabaseManager ! GetJobForCompletion(id, completionFunction)
            }
          }
        } ~
        path("run") {
          post {
            // force a job to run
            produce(instanceOf[Option[DropJob]]) { completionFunction =>
              context =>
                dropSupervisor ! RunJobImmediately(id, completionFunction)
            }
          }
        } ~
        path("children") {
          get {
            // list all dependencies for a job
            produce(instanceOf[DropJobList]) { completionFunction =>
              context =>
                jobDatabaseManager ! GetChildrenWithJobIDForCompletion(id, completionFunction)
            }
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
      anyParams(
          'status.as[LogStatus].?,
          'period.as(String2PositiveInt).?,
          'jobid.as(String2PositiveInt).?,
          'dropuid.as[String].?,
          'limit.as(String2NonNegativeInt).?,
          'offset.as(String2NonNegativeInt).?) { (status, period, jobID, dropUID, limit, offset) =>
        get {
          produce(instanceOf[DropHistory]) { completionFunction =>
            context =>
              jobDatabaseManager ! GetLogsForCompletion(jobID, period, status, dropUID, limit, offset, completionFunction)
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