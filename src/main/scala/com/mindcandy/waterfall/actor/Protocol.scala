package com.mindcandy.waterfall.actor

import java.sql.Timestamp

import org.joda.time.DateTime
import argonaut._
import Argonaut._
import com.mindcandy.waterfall.WaterfallDropFactory
import WaterfallDropFactory.DropUID
import scalaz.\/
import scala.language.implicitConversions
import scala.slick.driver.SQLiteDriver.simple._

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

object Protocol {
  case class DropJob(jobID: Option[Int], dropUID: DropUID, name: String, description: String, enabled: Boolean, cron: String, timeFrame: TimeFrame.TimeFrame, configuration: Map[String, String])
  case class DropJobList(jobs: List[DropJob])
  case class DropLog(logID: Option[Int], jobID: Int, startTime: DateTime, endTime: Option[DateTime], logOutput: Option[String], exception: Option[String])
  case class DropHistory(logs: List[DropLog])

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

  implicit val stackTraceElementEncode: EncodeJson[StackTraceElement] = EncodeJson(element =>
    jString(s"${element.getClassName}.${element.getMethodName}(${element.getFileName}:${element.getLineNumber})")
  )
  implicit val throwableEncode: EncodeJson[Throwable] = EncodeJson(error =>
    ("message" := jString(error.getMessage)) ->:
      ("stackTrace" := error.getStackTrace.toList.asJson) ->:
      jEmptyObject
  )
  implicit val throwableDecode: DecodeJson[Throwable] = optionDecoder(json =>
    for {
      message <- json.field("message")
      str <- message.string
      exception <- Option(new Exception(str))
    } yield exception, "Exception")

  implicit def DropJobCodecJson = casecodec8(DropJob.apply, DropJob.unapply)(
    "jobID", "dropUID", "name", "description", "enabled", "cron", "timeFrame", "configuration")
  implicit def DropLogCodecJson = casecodec6(DropLog.apply, DropLog.unapply)(
    "logID", "jobID", "startTime", "endTime", "logOutput", "exception")

  implicit val dateTimeColumnType = MappedColumnType.base[DateTime, Timestamp](
    { dt => new Timestamp(dt.getMillis) },
    { ts => new DateTime(ts) }
  )

  class DropLogs(tag: Tag) extends Table[DropLog](tag, "DROP_LOG") {
    def logID = column[Int]("LOG_ID", O.PrimaryKey, O.AutoInc)
    def jobID = column[Int]("JOB_ID", O.NotNull)
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
    def jobID = column[Int]("JOB_ID", O.PrimaryKey, O.AutoInc)
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
}