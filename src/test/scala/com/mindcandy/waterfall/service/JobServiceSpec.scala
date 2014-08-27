package com.mindcandy.waterfall.service

import com.mindcandy.waterfall.TestDatabase
import com.mindcandy.waterfall.actor.Protocol.{ DropHistory, DropJob, DropJobList }
import com.mindcandy.waterfall.actor.{ JobDatabaseManager, TimeFrame }
import org.specs2.specification.Grouped
import org.specs2.specification.script.Specification
import spray.http.StatusCode.int2StatusCode
import spray.routing.MalformedRequestContentRejection
import spray.testkit.Specs2RouteTest

class JobServiceSpec extends Specification with Grouped with Specs2RouteTest with JobService with TestDatabase {
  def is = s2"""
  JobService test
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
  """
  def actorRefFactory = system

  def jobDatabaseManager = {
    val db = testDatabase
    system.actorOf(JobDatabaseManager.props(db))
  }

  def getJobs = Get("/jobs") ~> route ~> check {
    responseAs[DropJobList] === DropJobList(
      Map(
        1 -> DropJob(Some(1), "EXRATE1", "Exchange Rate", "desc", true, "0 1 * * * ?", TimeFrame.DAY_YESTERDAY, Map()),
        2 -> DropJob(Some(2), "EXRATE2", "Exchange Rate", "desc", false, "0 1 * * * ?", TimeFrame.DAY_YESTERDAY, Map())
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
      Map(
        1 -> DropJob(Some(1), "EXRATE1", "Exchange Rate", "desc", true, "0 1 * * * ?", TimeFrame.DAY_YESTERDAY, Map())
      )
    )
  }

  def getJobsWithUnknownDropUID = Get("/drops/UNKNOWN/jobs") ~> route ~> check {
    responseAs[DropJobList] === DropJobList(Map())
  }

  def getSchedule = Get("/schedule") ~> route ~> check {
    responseAs[DropJobList] === DropJobList(
      Map(
        1 -> DropJob(Some(1), "EXRATE1", "Exchange Rate", "desc", true, "0 1 * * * ?", TimeFrame.DAY_YESTERDAY, Map())
      )
    )
  }

  def getStatusInfo = Get("/status/info") ~> route ~> check {
    status === int2StatusCode(200)
  }

  def getLogs = Get("/logs") ~> route ~> check {
    responseAs[DropHistory].count === 16
  }

  def getLogsWithJobID = Get("/logs?jobid=1") ~> route ~> check {
    responseAs[DropHistory].count === 8
    responseAs[DropHistory].logs.count(_.jobID == 1) === 8
  }

  def getLogsWithPeriod = Get("/logs?period=1") ~> route ~> check {
    responseAs[DropHistory].count === 8
  }
}
