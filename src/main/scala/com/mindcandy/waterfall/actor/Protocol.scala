package com.mindcandy.waterfall.actor

import org.joda.time.DateTime
import argonaut._
import Argonaut._
import com.mindcandy.waterfall.WaterfallDropFactory
import WaterfallDropFactory.DropUID
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

object Protocol {
  case class DropJob(dropUID: DropUID, name: String, enabled: Boolean, cron: String, timeFrame: TimeFrame.TimeFrame, configuration: Map[String, String])
  case class DropJobList(jobs: List[DropJob])
  case class DropLog(dropUID: DropUID, startTime: DateTime, endTime: Option[DateTime], logOutput: Option[String], exception: Option[Throwable])
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

  implicit def DropJobCodecJson = casecodec6(DropJob.apply, DropJob.unapply)("dropUID", "name", "enabled", "cron", "timeFrame", "configuration")
  implicit def DropLogCodecJson = casecodec5(DropLog.apply, DropLog.unapply)("dropUID", "startTime", "endTime", "logOutput", "exception")
}