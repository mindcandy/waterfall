package com.mindcandy.waterfall.actor

import org.joda.time.DateTime
import argonaut._
import Argonaut._
import com.mindcandy.waterfall.WaterfallDropFactory.DropUID
import scala.util.Try

object Protocol {
  case class DropJob(dropUID: DropUID, name: String, enabled: Boolean, cron: String)
  case class DropJobList(jobs: List[DropJob])
  case class DropLog(dropUID: DropUID, startTime: DateTime, endTime: Option[DateTime], logOutput: Option[String], exception: Option[Throwable])
  case class DropHistory(logs: List[DropLog])
  
  implicit val DateTimeEncodeJson: EncodeJson[DateTime] = EncodeJson(a => jString(a.toString))
  implicit val OptionDateTimeEncodeJson: EncodeJson[Option[DateTime]] = OptionEncodeJson(DateTimeEncodeJson)
  implicit val DateTimeDecodeJson: DecodeJson[DateTime] = optionDecoder((json =>
    for {
      str <- json.string
      date <- Try(DateTime.parse(str)).toOption
    } yield date
  ), "org.joda.time.DateTime")
  implicit val OptionDateTimeDecodeJson: DecodeJson[Option[DateTime]] = OptionDecodeJson(DateTimeDecodeJson)
  
  implicit val stackTraceElementEncode: EncodeJson[StackTraceElement] = EncodeJson(element =>
    jString(s"${element.getClassName}.${element.getMethodName}(${element.getFileName}:${element.getLineNumber})")
  )
  implicit val throwableEncode: EncodeJson[Throwable] = EncodeJson(error =>
    ("message" := jString(error.getMessage)) ->:
    ("stackTrace" := error.getStackTrace().toList.asJson) ->:
    jEmptyObject
  )
  implicit val throwableDecode: DecodeJson[Throwable] = optionDecoder((json =>
    for {
      message <- json.field("message")
      str <- message.string
      exception <- Option(new Exception(str))
    } yield exception
  ), "Exception")
  
  implicit def DropJobCodecJson = casecodec4(DropJob.apply, DropJob.unapply)("dropUID", "name", "enabled", "cron")
  implicit def DropLogCodecJson: EncodeJson[DropLog] = casecodec5(DropLog.apply, DropLog.unapply)("dropUID", "startTime", "endTime", "logOutput", "exception")
}