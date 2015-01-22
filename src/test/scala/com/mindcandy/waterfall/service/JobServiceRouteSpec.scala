package com.mindcandy.waterfall.service

import akka.actor.ActorRef
import com.github.nscala_time.time.Imports._
import com.mindcandy.waterfall.actor.Protocol.{ DropHistory, DropJob, DropJobList }
import com.mindcandy.waterfall.actor.{ DropSupervisor, JobDatabaseManager, TimeFrame }
import com.mindcandy.waterfall.{ TestDatabase, TestWaterfallDropFactory }
import org.specs2.ScalaCheck
import org.specs2.specification.Grouped
import org.specs2.specification.script.Specification
import org.specs2.time.NoTimeConversions
import spray.http.StatusCode.int2StatusCode
import spray.routing.{ ValidationRejection, MalformedQueryParamRejection, MalformedRequestContentRejection }
import spray.testkit.Specs2RouteTest

class JobServiceRouteSpec extends Specification with ScalaCheck with Grouped with Specs2RouteTest with TestDatabase with ArgonautMarshallers with NoTimeConversions {

  def route = {
    val jobDatabaseManager: ActorRef = {
      val db = testDatabaseWithJobsAndLogs
      system.actorOf(JobDatabaseManager.props(db))
    }
    val dropSupervisor: ActorRef = system.actorOf(DropSupervisor.props(jobDatabaseManager, new TestWaterfallDropFactory))
    JobServiceRoute(jobDatabaseManager, dropSupervisor).route
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
    get /jobs/:id/children ${getChildrenWithJobID}
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
    GET /logs?limit=10 ${getLogsWithLimit}
    GET /logs?offset=3 ${getLogsWithOffset}
    GET /logs?dropUID=EXRATE1 ${getLogsWithUnknownParameter}
    GET /logs?dropuid=EXRATE1 ${getLogsWithDropUID}
    POST /jobs/1/run ${postRunJob}
    POST /jobs/100/run with unknown jobID ${postRunJobWithUnknownJobID}

    POST /jobs/1 with cron and parents ${postJobWithCronAndParents}
    POST /jobs/1 with no cron or parents ${postJobWithNoCronOrParents}
    POST /jobs/1 with one parent ${postJobsWithParents}
    POST /jobs/1 with two parent ${postJobsWithTwoParents}

  """

  def getJobs = Get("/jobs") ~> route ~> check {
    responseAs[DropJobList] === DropJobList(testDropJobsWithCronParents)
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
        |     "configuration":{},
        |     "parallel": false
        |    }
      """.stripMargin
    Post("/jobs", postJson) ~> route ~> check {
      responseAs[DropJob] === DropJob(Some(4), "EXRATE3", "Exchange Rate", "yes", true, Option("0 1 * * * ?"), TimeFrame.DAY_YESTERDAY, Map(), false)
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
        |     "configuration":{},
        |     "parallel": true
        |    }
      """.stripMargin
    Post("/jobs", postJson) ~> route ~> check {
      responseAs[DropJob] === DropJob(Some(4), "EXRATE3", "Exchange Rate", "yes", true, Option("0 1 * * * ?"), TimeFrame.DAY_YESTERDAY, Map(), true)
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
        |     "configuration":{},
        |     "parallel": false
        |    }
      """.stripMargin
    Post("/jobs", postJson) ~> route ~> check {
      responseAs[DropJob] === DropJob(Some(2), "EXRATE3", "Exchange Rate", "yes", true, Option("0 1 * * * ?"), TimeFrame.DAY_YESTERDAY, Map(), false)
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
        |     "configuration":{},
        |     "parallel": true
        |    }
      """.stripMargin
    Post("/jobs", postJson) ~> route ~> check {
      // TODO(deo.liang): should be "failed to insert" instead of 404
      status === int2StatusCode(404)
    }
  }

  def getJobsWithJobID = Get("/jobs/1") ~> route ~> check {
    responseAs[DropJob] === testDropJobs(0)
  }

  def getChildrenWithJobID = Get("/jobs/1/children") ~> route ~> check {
    status === int2StatusCode(200)
  }

  def getJobsWithUnknownJobID = Get("/jobs/4") ~> route ~> check {
    status === int2StatusCode(404)
  }

  def getJobsWithDropUID = Get("/drops/EXRATE1/jobs") ~> route ~> check {
    responseAs[DropJobList] === DropJobList(testDropJobs.take(1))
  }

  def getJobsWithUnknownDropUID = Get("/drops/UNKNOWN/jobs") ~> route ~> check {
    responseAs[DropJobList] === DropJobList(List())
  }

  def getSchedule = Get("/schedule") ~> route ~> check {
    responseAs[DropJobList] === DropJobList(testDropJobs.filter(_.enabled))
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

  def getLogsWithUnknownParameter = Get("/logs?dropUID=EXRATE1") ~> route ~> check {
    // TODO(deo.liang): support case insensitive params.
    pending
  }

  def getLogsWithDropUID = Get("/logs?dropuid=EXRATE1") ~> route ~> check {
    val dropHistory = responseAs[DropHistory]
    (dropHistory.count === 8) and (dropHistory.logs.count(_.jobID == 1) === 8)
  }

  def getLogsWithLimit = Get("/logs?limit=10") ~> route ~> check {
    responseAs[DropHistory].count === 10
  }

  def getLogsWithOffset = Get("/logs?offset=3") ~> route ~> check {
    responseAs[DropHistory].logs === testDropLogs.sortBy(x => (-x.startTime.millis, x.jobID, x.runUID.toString)).drop(3)
  }

  def postRunJob = Post("/jobs/1/run") ~> route ~> check {
    responseAs[Option[DropJob]] === Option(testDropJobs(0))
  }

  def postRunJobWithUnknownJobID = Post("/jobs/100/run") ~> route ~> check {
    status === int2StatusCode(404)
  }

  def postJobWithCronAndParents = {
    val postJson =
      """
        |{
        |     "dropUID":"EXRATE3",
        |     "name":"Exchange Rate",
        |     "description": "yes",
        |     "enabled":true,
        |     "cron":"0 1 * * * ?",
        |     "timeFrame":"DAY_YESTERDAY",
        |     "configuration":{},
        |     "parallel": false,
        |     "parents":[1]
        |    }
      """.stripMargin
    Post("/jobs", postJson) ~> route ~> check {
      rejection === ValidationRejection("A job can only have a cron or one parent", None)
    }
  }

  def postJobWithNoCronOrParents = {
    val postJson =
      """
        |{
        |     "dropUID":"EXRATE3",
        |     "name":"Exchange Rate",
        |     "description": "yes",
        |     "enabled":true,
        |     "timeFrame":"DAY_YESTERDAY",
        |     "configuration":{},
        |     "parallel": false
        |    }
      """.stripMargin
    Post("/jobs", postJson) ~> route ~> check {
      rejection === ValidationRejection("A job can only have a cron or one parent", None)
    }
  }

  def postJobsWithParents = {
    val postJson =
      """
        |{
        |     "dropUID":"EXRATE3",
        |     "name":"Exchange Rate",
        |     "description": "yes",
        |     "enabled":true,
        |     "timeFrame":"DAY_YESTERDAY",
        |     "configuration":{},
        |     "parallel": false,
        |     "parents":[1]
        |    }
      """.stripMargin
    Post("/jobs", postJson) ~> route ~> check {
      responseAs[DropJob] === DropJob(Some(4), "EXRATE3", "Exchange Rate", "yes", true, Option.empty[String], TimeFrame.DAY_YESTERDAY, Map(), false, Option(List(1)))
    }
  }

  def postJobsWithTwoParents = {
    val postJson =
      """
        |{
        |     "dropUID":"EXRATE3",
        |     "name":"Exchange Rate",
        |     "description": "yes",
        |     "enabled":true,
        |     "timeFrame":"DAY_YESTERDAY",
        |     "configuration":{},
        |     "parallel": false,
        |     "parents":[1,5]
        |    }
      """.stripMargin
    Post("/jobs", postJson) ~> route ~> check {
      rejection === ValidationRejection("A job can only have a cron or one parent", None)
    }
  }
}
