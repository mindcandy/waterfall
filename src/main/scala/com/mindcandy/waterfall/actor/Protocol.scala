package com.mindcandy.waterfall.actor

import java.sql.Timestamp
import java.util.UUID

import argonaut.Argonaut._
import argonaut._
import com.github.nscala_time.time.Imports._
import com.mindcandy.waterfall.WaterfallDropFactory.DropUID
import com.mindcandy.waterfall.config.{ DatabaseConfig, DatabaseContainer }
import org.joda.time.DateTime
import org.quartz.CronExpression

import scala.language.implicitConversions
import scala.util.{ Try, Success }
import scalaz.\/

object TimeFrame extends Enumeration {
  type TimeFrame = Value
  val DAY_TODAY, DAY_YESTERDAY, DAY_TWO_DAYS_AGO, DAY_THREE_DAYS_AGO = Value

  implicit val TimeFrameEncodeJson: EncodeJson[TimeFrame] = EncodeJson(a => jString(a.toString))
  implicit val TimeFrameDecodeJson: DecodeJson[TimeFrame] = DecodeJson(hcursor =>
    DecodeResult(hcursor.as[String].result.flatMap { value =>
      \/.fromTryCatch { TimeFrame.withName(value) }.leftMap { exception =>
        (s"Invalid TimeFrame value: $value", CursorHistory(List.empty[CursorOp]))
      }
    })
  )
}

object LogStatus extends Enumeration {
  type LogStatus = Value
  val RUNNING, FAILURE, SUCCESS = Value

  implicit val TimeFrameEncodeJson: EncodeJson[LogStatus] = EncodeJson(a => jString(a.toString))
  implicit val TimeFrameDecodeJson: DecodeJson[LogStatus] = DecodeJson(hcursor =>
    DecodeResult(hcursor.as[String].result.flatMap { value =>
      \/.fromTryCatch { LogStatus.withName(value) }.leftMap { exception =>
        (s"Invalid LogStatus value: $value", CursorHistory(List.empty[CursorOp]))
      }
    })
  )
}

object Protocol {
  type JobID = Int
  case class DropJob(jobID: Option[JobID], dropUID: DropUID, name: String, description: String, enabled: Boolean, cron: String, timeFrame: TimeFrame.TimeFrame, configuration: Map[String, String])
  case class DropJobList(jobs: Map[JobID, DropJob]) {
    val count = jobs.size
  }
  case class DropLog(runUID: UUID, jobID: JobID, startTime: DateTime, endTime: Option[DateTime], logOutput: Option[String], exception: Option[String])
  case class DropHistory(logs: List[DropLog]) {
    val count = logs.size
  }

  implicit val DateTimeEncodeJson: EncodeJson[DateTime] = EncodeJson(a => jString(a.toString))
  implicit val OptionDateTimeEncodeJson: EncodeJson[Option[DateTime]] = OptionEncodeJson(DateTimeEncodeJson)
  implicit val DateTimeDecodeJson: DecodeJson[DateTime] = DecodeJson(hcursor =>
    DecodeResult(hcursor.as[String].result.flatMap { value =>
      \/.fromTryCatch { DateTime.parse(value) }.leftMap { exception =>
        (exception.getMessage, CursorHistory(List.empty[CursorOp]))
      }
    })
  )
  implicit val OptionDateTimeDecodeJson: DecodeJson[Option[DateTime]] = OptionDecodeJson(DateTimeDecodeJson)

  implicit def DropJobCodecJson = casecodec8(DropJob.apply, DropJob.unapply)(
    "jobID", "dropUID", "name", "description", "enabled", "cron", "timeFrame", "configuration")
  implicit def DropLogCodecJson = casecodec6(DropLog.apply, DropLog.unapply)(
    "logID", "jobID", "startTime", "endTime", "logOutput", "exception")
  implicit def DropJobListCodecJson: CodecJson[DropJobList] = CodecJson(
    (dropJobList: DropJobList) =>
      ("count" := dropJobList.count) ->:
        ("jobs" := dropJobList.jobs.values.toList) ->:
        jEmptyObject,
    json => for {
      jobs <- (json --\ "jobs").as[List[DropJob]]
    } yield DropJobList(jobs.map(x => x.jobID.getOrElse(-1) -> x).toMap)
  )
  implicit def DropHistoryCodecJson: CodecJson[DropHistory] = CodecJson(
    (dropHistory: DropHistory) =>
      ("count" := dropHistory.count) ->:
        ("logs" := dropHistory.logs) ->:
        jEmptyObject,
    json => for {
      logs <- (json --\ "logs").as[List[DropLog]]
    } yield DropHistory(logs)
  )
}

class DB(val config: DatabaseConfig) extends DatabaseContainer {
  val driver = config.driver
  import com.mindcandy.waterfall.actor.Protocol._
  import driver.simple._

  import scala.slick.jdbc.JdbcBackend.Database.dynamicSession

  val db = Database.forURL(
    config.url, config.username, config.password, driver = config.driverClass
  )

  implicit val dateTimeColumnType = MappedColumnType.base[DateTime, Timestamp](
    { dt => new Timestamp(dt.getMillis) },
    { ts => new DateTime(ts) }
  )

  class DropLogs(tag: Tag) extends Table[DropLog](tag, "DROP_LOG") {
    def logID = column[Int]("LOG_ID", O.PrimaryKey, O.AutoInc)
    def jobID = column[JobID]("JOB_ID", O.NotNull)
    def startTime = column[DateTime]("START_TIME", O.NotNull)
    def endTime = column[Option[DateTime]]("END_TIME")
    def content = column[Option[String]]("CONTENT")
    def exception = column[Option[String]]("EXCEPTION")
    def * =
      (logID.?, jobID, startTime, endTime, content, exception) <>
        (DropLog.tupled, DropLog.unapply)
    def job_fk = foreignKey("JOB_FK", jobID, dropJobs)(_.jobID)
  }

  val dropLogs = TableQuery[DropLogs]

  implicit val timeFrameColumnType = MappedColumnType.base[TimeFrame.TimeFrame, String](
    { tf => tf.toString },
    { ts => TimeFrame.withName(ts) }
  )

  implicit val MapStringStringColumnType = MappedColumnType.base[Map[String, String], String](
    { m => m.asJson.nospaces },
    { ts =>
      val opt = Parse.decodeOption[Map[String, String]](ts)
      // TODO(deo.liang): the stored value should be a valid json string, probably
      //                  we can just do Parse.decode
      opt.getOrElse(Map[String, String]())
    }
  )

  class DropJobs(tag: Tag) extends Table[DropJob](tag, "DROP_JOB") {
    def jobID = column[JobID]("JOB_ID", O.PrimaryKey, O.AutoInc)
    def dropUID = column[String]("DROP_UID", O.NotNull)
    def name = column[String]("NAME", O.NotNull)
    def description = column[String]("DESCRIPTION", O.NotNull)
    def enabled = column[Boolean]("ENABLED", O.NotNull)
    def cron = column[String]("CRON", O.NotNull)
    def timeFrame = column[TimeFrame.TimeFrame]("TIME_FRAME", O.NotNull)
    // configuration stored as a json string
    def configuration = column[Map[String, String]]("CONFIGURATION", O.NotNull)
    def * =
      (jobID.?, dropUID, name, description, enabled, cron, timeFrame, configuration) <>
        (DropJob.tupled, DropJob.unapply)
  }

  val dropJobs = TableQuery[DropJobs]
  val all = Seq(dropJobs, dropLogs)

  def maybeExists(dropJob: DropJob): Option[DropJob] =
    dropJob.jobID.flatMap(jid => dropJobs.filter(_.jobID === jid).firstOption)

  def insertAndReturnDropJob(dropJob: DropJob): Option[DropJob] = {
    (dropJobs returning dropJobs.map(_.jobID) into ((job, id) => Some(job.copy(jobID = Some(id))))) += dropJob
  }

  def updateAndReturn(dropJob: DropJob): Option[DropJob] = {
    val job = dropJobs.filter(_.jobID === dropJob.jobID)
    job.update(dropJob)
    job.firstOption
  }

  def insertOrUpdateDropJob(dropJob: DropJob): Option[DropJob] = {
    CronExpression.isValidExpression(dropJob.cron) match {
      case false => None
      case true => maybeExists(dropJob).fold(insertAndReturnDropJob(dropJob))(_ => updateAndReturn(dropJob))
    }
  }

  def selectDropLog(jobID: Option[JobID], time: Option[Int], status: Option[String]): List[DropLog] = {
    // TODO(deo.liang): use intersect when it's available in slick2.2
    val resultFilterTime = time.fold(dropLogs.sortBy(_.startTime.desc)) { time =>
      val timeFrom = DateTime.now - time.hour
      dropLogs
        .filter(x =>
          (x.endTime.isDefined && x.endTime >= timeFrom) ||
            (x.endTime.isEmpty && x.startTime >= timeFrom))
        .sortBy(_.startTime.desc)
    }
    val resultFilterJobID = jobID.fold(resultFilterTime)(id => resultFilterTime.filter(_.jobID === id))
    Try(LogStatus.withName(status.get.toUpperCase)) match {
      case Success(LogStatus.FAILURE) => resultFilterJobID.filter(_.exception.isDefined).list
      case Success(LogStatus.SUCCESS) => resultFilterJobID.filter(_.exception.isEmpty).list
      case Success(LogStatus.RUNNING) => resultFilterJobID.filter(_.endTime.isEmpty).list
      case _ => resultFilterJobID.list
    }
  }
}