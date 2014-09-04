package com.mindcandy.waterfall.service

import com.github.nscala_time.time.Imports._
import akka.actor.ActorRef
import com.mindcandy.waterfall.TestDatabase
import com.mindcandy.waterfall.actor.Protocol.{DropHistory, DropJob, DropJobList}
import com.mindcandy.waterfall.actor.{JobDatabaseManager, TimeFrame}
import org.specs2.ScalaCheck
import org.specs2.specification.Grouped
import org.specs2.specification.script.Specification
import org.specs2.time.NoTimeConversions
import spray.http.StatusCode.int2StatusCode
import spray.routing.{ MalformedQueryParamRejection, MalformedRequestContentRejection }
import spray.testkit.Specs2RouteTest

class JobServiceRouteSpec extends Specification with ScalaCheck with Grouped with Specs2RouteTest with TestDatabase with ArgonautMarshallers with NoTimeConversions {

  def route = {
    implicit val jobDatabaseManager: ActorRef = {
      val db = testDatabaseWithJobsAndLogs
      system.actorOf(JobDatabaseManager.props(db))
    }
    JobServiceRoute().route
  }

  def is = s2"""
  JobServiceRoute test
  ==============================================================================
    get /jobs ${getJobs}
    post /jobs with no header nor body ${postJobs}
    post /jobs with json without jobID ${postJobsWithoutJobID}
    post /jobs with json with unknown jobID ${postJobsWithUnknownJobID}
    post /jobs with json with existing jobID ${postJobsWithExistingJobID}
    post /jobs with json with malformed cron ${postJobsWithMalformedCron}
    get /jobs/:id ${getJobsWithJobID}
    get /jobs/:id with unknown id ${getJobsWithUnknownJobID}
    get /drops/:dropuid/jobs ${getJobsWithDropUID}
    get /drops/:dropuid/jobs with unknown dropUID ${getJobsWithUnknownDropUID}
    get /schedule ${getSchedule}
    get /status/info ${getStatusInfo}
    get /logs ${getLogs}
    get /logs?jobid=1 ${getLogsWithJobID}
    get /logs?period=1 ${getLogsWithPeriod}
    get /logs?period=-1 ${getLogsWithNegativePeriod}
    get /logs?status=success ${getLogsWithStatusSuccess}
    get /logs?status=failure ${getLogsWithStatusFailure}
    get /logs?status=running ${getLogsWithStatusRunning}
    get /logs?status=ruNNing ${getLogsWithStatusRunningCaseInsensitive}
    get /logs?status=unknown wrong status ${getLogsWithUnknownStatus}
    GET /logs?dropUID=EXRATE1 ${getLogsWithDropUID}
    GET /logs?dropuid=EXRATE1 ${getLogsWithUnknownParameter}
  """

  def getJobs = Get("/jobs") ~> route ~> check {
    responseAs[DropJobList] === DropJobList(
      List(
        DropJob(Some(1), "EXRATE1", "Exchange Rate", "desc", true, "0 1 * * * ?", TimeFrame.DAY_YESTERDAY, Map()),
        DropJob(Some(2), "EXRATE2", "Exchange Rate", "desc", false, "0 1 * * * ?", TimeFrame.DAY_YESTERDAY, Map())
      )
    )
  }

  def postJobs = Post("/jobs") ~> route ~> check {
    rejection === MalformedRequestContentRejection("JSON terminates unexpectedly", None)
  }

  def postJobsWithoutJobID = {
    val postJson =
      """
        |{
        |     "dropUID":"EXRATE3",
        |     "name":"Exchange Rate",
        |     "description": "yes",
        |     "enabled":true,
        |     "cron":"0 1 * * * ?",
        |     "timeFrame":"DAY_YESTERDAY",
        |     "configuration":{}
        |    }
      """.stripMargin
    Post("/jobs", postJson) ~> route ~> check {
      responseAs[DropJob] === DropJob(Some(3), "EXRATE3", "Exchange Rate", "yes", true, "0 1 * * * ?", TimeFrame.DAY_YESTERDAY, Map())
    }
  }

  def postJobsWithUnknownJobID = {
    val postJson =
      """
        |{
        |     "jobID": 10,
        |     "dropUID":"EXRATE3",
        |     "name":"Exchange Rate",
        |     "description": "yes",
        |     "enabled":true,
        |     "cron":"0 1 * * * ?",
        |     "timeFrame":"DAY_YESTERDAY",
        |     "configuration":{}
        |    }
      """.stripMargin
    Post("/jobs", postJson) ~> route ~> check {
      responseAs[DropJob] === DropJob(Some(3), "EXRATE3", "Exchange Rate", "yes", true, "0 1 * * * ?", TimeFrame.DAY_YESTERDAY, Map())
    }
  }

  def postJobsWithExistingJobID = {
    val postJson =
      """
        |{
        |     "jobID": 2,
        |     "dropUID":"EXRATE3",
        |     "name":"Exchange Rate",
        |     "description": "yes",
        |     "enabled":true,
        |     "cron":"0 1 * * * ?",
        |     "timeFrame":"DAY_YESTERDAY",
        |     "configuration":{}
        |    }
      """.stripMargin
    Post("/jobs", postJson) ~> route ~> check {
      responseAs[DropJob] === DropJob(Some(2), "EXRATE3", "Exchange Rate", "yes", true, "0 1 * * * ?", TimeFrame.DAY_YESTERDAY, Map())
    }
  }

  def postJobsWithMalformedCron = {
    val postJson =
      """
        |{
        |     "jobID": 2,
        |     "dropUID":"EXRATE3",
        |     "name":"Exchange Rate",
        |     "description": "yes",
        |     "enabled":true,
        |     "cron":"malformed cron",
        |     "timeFrame":"DAY_YESTERDAY",
        |     "configuration":{}
        |    }
      """.stripMargin
    Post("/jobs", postJson) ~> route ~> check {
      // TODO(deo.liang): should be "failed to insert" instead of 404
      status === int2StatusCode(404)
    }
  }

  def getJobsWithJobID = Get("/jobs/1") ~> route ~> check {
    responseAs[DropJob] === DropJob(Some(1), "EXRATE1", "Exchange Rate", "desc", true, "0 1 * * * ?", TimeFrame.DAY_YESTERDAY, Map())
  }

  def getJobsWithUnknownJobID = Get("/jobs/3") ~> route ~> check {
    status === int2StatusCode(404)
  }

  def getJobsWithDropUID = Get("/drops/EXRATE1/jobs") ~> route ~> check {
    responseAs[DropJobList] === DropJobList(
      List(
        DropJob(Some(1), "EXRATE1", "Exchange Rate", "desc", true, "0 1 * * * ?", TimeFrame.DAY_YESTERDAY, Map())
      )
    )
  }

  def getJobsWithUnknownDropUID = Get("/drops/UNKNOWN/jobs") ~> route ~> check {
    responseAs[DropJobList] === DropJobList(List())
  }

  def getSchedule = Get("/schedule") ~> route ~> check {
    responseAs[DropJobList] === DropJobList(
      List(
        DropJob(Some(1), "EXRATE1", "Exchange Rate", "desc", true, "0 1 * * * ?", TimeFrame.DAY_YESTERDAY, Map())
      )
    )
  }

  def getStatusInfo = Get("/status/info") ~> route ~> check {
    status === int2StatusCode(200)
  }

  def getLogs = Get("/logs") ~> route ~> check {
    responseAs[DropHistory].logs === testDropLogs.sortBy(x => (-x.startTime.millis, x.jobID, x.runUID.toString))
  }

  def getLogsWithJobID = Get("/logs?jobid=1") ~> route ~> check {
    val dropHistory = responseAs[DropHistory]
    (dropHistory.count === 8) and (dropHistory.logs.count(_.jobID == 1) === 8)
  }

  def getLogsWithPeriod = Get("/logs?period=1") ~> route ~> check {
    val dropHistory = responseAs[DropHistory]
    (dropHistory.count === 8) and (dropHistory.logs.count(log => log.endTime match {
      case Some(endTime) => endTime > DateTime.now - 1.hour
      case None => log.startTime > DateTime.now - 1.hour
    }) === 8)
  }

  def getLogsWithNegativePeriod = Get("/logs?period=-1") ~> route ~> check {
    rejection === MalformedQueryParamRejection("period", "'-1' is not a valid positive integer", None)
  }

  def getLogsWithStatusSuccess = Get("/logs?status=success") ~> route ~> check {
    val dropHistory = responseAs[DropHistory]
    (dropHistory.count === 8) and (dropHistory.logs.count(_.exception.isEmpty) === 8)
  }

  def getLogsWithStatusFailure = Get("/logs?status=failure") ~> route ~> check {
    val dropHistory = responseAs[DropHistory]
    (dropHistory.count === 8) and (dropHistory.logs.count(_.exception.isDefined) === 8)
  }

  def getLogsWithStatusRunning = Get("/logs?status=running") ~> route ~> check {
    val dropHistory = responseAs[DropHistory]
    (dropHistory.count === 8) and (dropHistory.logs.count(_.endTime.isEmpty) === 8)
  }

  def getLogsWithStatusRunningCaseInsensitive = Get("/logs?status=ruNNing") ~> route ~> check {
    val dropHistory = responseAs[DropHistory]
    (dropHistory.count === 8) and (dropHistory.logs.count(_.endTime.isEmpty) === 8)
  }

  def getLogsWithUnknownStatus = Get("/logs?status=unknown") ~> route ~> check {
    rejection === MalformedQueryParamRejection("status", "'unknown' is not a valid log status value", None)
  }

  def getLogsWithDropUID = Get("/logs?dropUID=EXRATE1") ~> route ~> check {
    // TODO(deo.liang): support case insensitive params.
    pending
  }

  def getLogsWithUnknownParameter = Get("/logs?dropuid=EXRATE1") ~> route ~> check {
    val dropHistory = responseAs[DropHistory]
    (dropHistory.count === 8) and (dropHistory.logs.count(_.jobID == 1) === 8)
  }
}
