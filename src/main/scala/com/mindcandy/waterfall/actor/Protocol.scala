package com.mindcandy.waterfall.actor

import java.util.UUID

import argonaut.Argonaut._
import argonaut._
import com.mindcandy.waterfall.WaterfallDropFactory.DropUID
import org.joda.time.DateTime
import spray.httpx.unmarshalling.{ Deserializer, MalformedContent }

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

  case class DropJobModel(jobID: Option[JobID],
                          dropUID: DropUID,
                          name: String,
                          description: String,
                          enabled: Boolean,
                          cron: Option[String],
                          timeFrame: TimeFrame.TimeFrame,
                          configuration: Map[String, String],
                          parallel: Boolean = false,
                          parents: Option[List[JobID]])

  case class DropJob(jobID: Option[JobID],
                     dropUID: DropUID,
                     name: String,
                     description: String,
                     enabled: Boolean,
                     cron: Option[String],
                     timeFrame: TimeFrame.TimeFrame,
                     configuration: Map[String, String],
                     parallel: Boolean = false) {
    lazy val children: List[DropJob] = ???
    lazy val parents: List[DropJob] = ???
  }
  //                     parents: List[JobID])
  case class DropJobList(jobs: List[DropJob]) {
    val count = jobs.size
  }
  case class DropJobMap(jobs: Map[JobID, DropJob]) {
    val count = jobs.size
  }
  case class DropLog(runUID: RunUID, jobID: JobID, startTime: DateTime, endTime: Option[DateTime], logOutput: Option[String], exception: Option[String])
  case class DropHistory(logs: List[DropLog]) {
    val count = logs.size
  }
  case class DropJobDependency(initiatorJobID: JobID, dependantJobID: JobID)

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

  implicit def DropJobModelCodecJson = casecodec10(DropJobModel.apply, DropJobModel.unapply)(
    "jobID", "dropUID", "name", "description", "enabled", "cron", "timeFrame", "configuration", "parallel", "parents")
  implicit def DropJobCodecJson = casecodec9(DropJob.apply, DropJob.unapply)(
    "jobID", "dropUID", "name", "description", "enabled", "cron", "timeFrame", "configuration", "parallel")
  implicit def DropLogCodecJson = casecodec6(DropLog.apply, DropLog.unapply)(
    "runID", "jobID", "startTime", "endTime", "logOutput", "exception")
  implicit def DropJobListCodecJson: CodecJson[DropJobList] = CodecJson(
    (dropJobList: DropJobList) =>
      ("count" := dropJobList.count) ->:
        ("jobs" := dropJobList.jobs) ->:
        jEmptyObject,
    json => for {
      jobs <- (json --\ "jobs").as[List[DropJob]]
    } yield DropJobList(jobs)
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

  val String2PositiveInt = new Deserializer[String, Int] {
    def apply(value: String) = Try(value.toInt) match {
      case Success(integer) if integer > 0 => Right(integer)
      case _ => Left(MalformedContent(s"'$value' is not a valid positive integer"))
    }
  }

  val String2NonNegativeInt = new Deserializer[String, Int] {
    def apply(value: String) = Try(value.toInt) match {
      case Success(integer) if integer >= 0 => Right(integer)
      case _ => Left(MalformedContent(s"'$value' is not a valid non-negative integer"))
    }
  }

  implicit def dropJobModel2DropJob(model: DropJobModel): DropJob = {
    DropJob(
      jobID = model.jobID,
      dropUID = model.dropUID,
      name = model.name,
      description = model.description,
      enabled = model.enabled,
      cron = model.cron,
      timeFrame = model.timeFrame,
      configuration = model.configuration,
      parallel = model.parallel
    )
  }
}
