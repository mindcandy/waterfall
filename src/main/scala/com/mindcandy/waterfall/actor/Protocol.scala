package com.mindcandy.waterfall.actor

import org.joda.time.DateTime
import argonaut._
import Argonaut._
import com.mindcandy.waterfall.drop.WaterfallDropFactory
import WaterfallDropFactory.DropUID
import scala.util.Try

object TimeFrame extends Enumeration {
  type TimeFrame = Value
  val CURRENT_DAY, PREVIOUS_DAY, CURRENT_WEEK, PREVIOUS_WEEK, CURRENT_MONTH, PREVIOUS_MONTH = Value

  implicit val TimeFrameEncodeJson: EncodeJson[TimeFrame] = EncodeJson(a => jString(a.toString))
  implicit val TimeFrameDecodeJson: DecodeJson[TimeFrame] = optionDecoder(json =>
    for {
      str <- json.string
      value <- Try(TimeFrame.withName(str)).toOption
    } yield value, "com.mindcandy.waterfall.actor.TimeFrame")
}

object Protocol {
  case class DropJob(dropUID: DropUID, name: String, enabled: Boolean, cron: String, timeFrame: TimeFrame.TimeFrame)
  case class DropJobList(jobs: List[DropJob])
  case class DropLog(dropUID: DropUID, startTime: DateTime, endTime: Option[DateTime], logOutput: Option[String], exception: Option[Throwable])
  case class DropHistory(logs: List[DropLog])
  
  implicit val DateTimeEncodeJson: EncodeJson[DateTime] = EncodeJson(a => jString(a.toString))
  implicit val OptionDateTimeEncodeJson: EncodeJson[Option[DateTime]] = OptionEncodeJson(DateTimeEncodeJson)
  implicit val DateTimeDecodeJson: DecodeJson[DateTime] = optionDecoder(json =>
    for {
      str <- json.string
      date <- Try(DateTime.parse(str)).toOption
    } yield date, "org.joda.time.DateTime")
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
  
  implicit def DropJobCodecJson = casecodec5(DropJob.apply, DropJob.unapply)("dropUID", "name", "enabled", "cron", "timeFrame")
  implicit def DropLogCodecJson = casecodec5(DropLog.apply, DropLog.unapply)("dropUID", "startTime", "endTime", "logOutput", "exception")
}