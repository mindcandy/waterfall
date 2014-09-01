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
import spray.httpx.unmarshalling.{ MalformedContent, Deserializer }

import scala.language.implicitConversions
import scala.util.{ Failure, Success, Try }
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

  implicit val LogStatusEncodeJson: EncodeJson[LogStatus] = EncodeJson(a => jString(a.toString))
  implicit val LogStatusDecodeJson: DecodeJson[LogStatus] = DecodeJson(hcursor =>
    DecodeResult(hcursor.as[String].result.flatMap { value =>
      \/.fromTryCatch { LogStatus.withName(value) }.leftMap { exception =>
        (s"Invalid LogStatus value: $value", CursorHistory(List.empty[CursorOp]))
      }
    })
  )

  implicit val String2LogStatusConverter = new Deserializer[String, LogStatus] {
    def apply(value: String) = Try(LogStatus.withName(value.toUpperCase)) match {
      case Success(logStatus) => Right(logStatus)
      case Failure(_) => Left(MalformedContent("'" + value + "' is not a valid log status value"))
    }
  }
}

object Protocol {
  type JobID = Int
  type RunUID = UUID
  case class DropJob(jobID: Option[JobID], dropUID: DropUID, name: String, description: String, enabled: Boolean, cron: String, timeFrame: TimeFrame.TimeFrame, configuration: Map[String, String])
  case class DropJobList(jobs: Map[JobID, DropJob]) {
    val count = jobs.size
  }
  case class DropLog(runUID: RunUID, jobID: JobID, startTime: DateTime, endTime: Option[DateTime], logOutput: Option[String], exception: Option[String])
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
  implicit def UUIDCodecJson: CodecJson[UUID] = CodecJson(
    (uuid: UUID) =>
      jString(uuid.toString),
    hcursor =>
      DecodeResult(
        hcursor.as[String].result.flatMap { value =>
          \/.fromTryCatch { UUID.fromString(value) }.leftMap { exception =>
            (exception.getMessage, CursorHistory(List.empty[CursorOp]))
          }
        }
      )
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

  class DropLogs(tag: Tag) extends Table[DropLog](tag, "drop_log") {
    def runUID = column[RunUID]("run_id", O.PrimaryKey)
    def jobID = column[JobID]("job_id", O.NotNull)
    def startTime = column[DateTime]("start_time", O.NotNull)
    def endTime = column[Option[DateTime]]("end_time")
    def content = column[Option[String]]("content")
    def exception = column[Option[String]]("exception")
    def * =
      (runUID, jobID, startTime, endTime, content, exception) <>
        (DropLog.tupled, DropLog.unapply)
    def job_fk = foreignKey("drop_log_job_id_fk", jobID, dropJobs)(_.jobID)
  }

  val dropLogs = TableQuery[DropLogs]
  val dropLogsSorted = dropLogs.sortBy(_.startTime.desc)

  implicit val timeFrameColumnType = MappedColumnType.base[TimeFrame.TimeFrame, String](
    { tf => tf.toString },
    { ts => TimeFrame.withName(ts) }
  )

  implicit val MapStringStringColumnType = MappedColumnType.base[Map[String, String], String](
    { m => m.asJson.nospaces },
    { ts =>
      val opt = Parse.decodeOption[Map[String, String]](ts)
      // TODO(deo.liang): the stored value should be a valid json string, probably we can just do Parse.decode
      opt.getOrElse(Map[String, String]())
    }
  )

  class DropJobs(tag: Tag) extends Table[DropJob](tag, "drop_job") {
    def jobID = column[JobID]("job_id", O.PrimaryKey, O.AutoInc)
    def dropUID = column[String]("drop_uid", O.NotNull)
    def name = column[String]("name", O.NotNull)
    def description = column[String]("description", O.NotNull)
    def enabled = column[Boolean]("enabled", O.NotNull)
    def cron = column[String]("cron", O.NotNull)
    def timeFrame = column[TimeFrame.TimeFrame]("time_frame", O.NotNull)
    // configuration stored as a json string
    def configuration = column[Map[String, String]]("configuration", O.NotNull)
    def * =
      (jobID.?, dropUID, name, description, enabled, cron, timeFrame, configuration) <>
        (DropJob.tupled, DropJob.unapply)
  }

  val dropJobs = TableQuery[DropJobs]
  val dropJobsSorted = dropJobs.sortBy(_.jobID.asc).sortBy(_.dropUID.asc)
  val allTables = Seq(dropJobs, dropLogs)

  def maybeExists(dropJob: DropJob): Option[DropJob] =
    dropJob.jobID.flatMap(jid => dropJobs.filter(_.jobID === jid).firstOption)

  def insertAndReturnDropJob(dropJob: DropJob): Option[DropJob] = {
    (dropJobs returning dropJobs.map(_.jobID) into ((job, id) => Some(job.copy(jobID = Some(id))))) += dropJob
  }

  def updateAndReturnDropJob(dropJob: DropJob): Option[DropJob] = {
    val job = dropJobs.filter(_.jobID === dropJob.jobID)
    job.update(dropJob)
    job.firstOption
  }

  def updateDropLog(runUID: RunUID, endTime: DateTime, logOutput: Option[String], exception: Option[String]) = {
    dropLogs
      .filter(_.runUID === runUID)
      .map(log => (log.endTime, log.content, log.exception))
      .update((Some(endTime), logOutput, exception))
  }
  def insertOrUpdateDropJob(dropJob: DropJob): Option[DropJob] = {
    CronExpression.isValidExpression(dropJob.cron) match {
      case false => None
      case true => maybeExists(dropJob).fold(insertAndReturnDropJob(dropJob))(_ => updateAndReturnDropJob(dropJob))
    }
  }

  def selectDropLog(jobID: Option[JobID], period: Option[Int], status: Option[LogStatus.LogStatus], dropUID: Option[DropUID]): List[DropLog] = {
    type LogQuery = Query[(DropLogs, DropJobs), (DropLog, DropJob), Seq]
    val logJoin = (for {
      job <- dropJobs
      log <- dropLogs if job.jobID === log.jobID
    } yield (log, job)).sortBy { case (log, job) => log.startTime.desc }

    val filterByTime: Option[LogQuery => LogQuery] = period.map(p => { q: LogQuery =>
      val timeFrom = DateTime.now - p.hour
      q.filter {
        case (log, job) =>
          (log.endTime.isDefined && log.endTime >= timeFrom) || (log.endTime.isEmpty && log.startTime >= timeFrom)
      }
    })
    val filterByJobID: Option[LogQuery => LogQuery] = jobID.map(id => { q: LogQuery =>
      q.filter { case (log, job) => log.jobID === id }
    })
    val filterByDropUID: Option[LogQuery => LogQuery] = dropUID.map(uid => { q: LogQuery =>
      q.filter { case (log, job) => job.dropUID === uid }
    })
    val filterByStatus: Option[LogQuery => LogQuery] = status.map(s => { q: LogQuery =>
      s match {
        case LogStatus.FAILURE => q.filter { case (log, job) => log.exception.isDefined }
        case LogStatus.SUCCESS => q.filter { case (log, job) => log.exception.isEmpty }
        case LogStatus.RUNNING => q.filter { case (log, job) => log.endTime.isEmpty }
      }
    })

    val allFilters: List[LogQuery => LogQuery] = List(
      filterByTime, filterByJobID, filterByDropUID, filterByStatus
    ).flatten

    // Apply the functions, if any, to the query object
    allFilters.foldLeft(logJoin)((logs, func) => func(logs)).list.map(_._1)
  }
}