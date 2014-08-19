package com.mindcandy.waterfall

import com.typesafe.scalalogging.slf4j.Logging
import org.joda.time.DateTime

import scala.util.Try

trait WaterfallDrop[A <: AnyRef, B <: AnyRef] extends Logging {
  def source: IOSource[A]
  def sourceIntermediate: Intermediate[A]
  def sourceIntermediateFormat: IntermediateFormat[A]

  def sink: IOSink[B]
  def sinkIntermediate: Intermediate[B]
  def sinkIntermediateFormat: IntermediateFormat[B]

  def transform(sourceInter: Intermediate[A], sinkInter: Intermediate[B]): Try[Unit]

  def run: Try[Unit] = {
    for {
      _ <- source.retrieveInto(sourceIntermediate)(sourceIntermediateFormat)
      _ <- transform(sourceIntermediate, sinkIntermediate)
      _ <- sink.storeFrom(sinkIntermediate)(sinkIntermediateFormat)
    } yield ()
  }
}

object WaterfallDropFactory {
  type DropUID = String
}

trait WaterfallDropFactory {
  import com.mindcandy.waterfall.WaterfallDropFactory._

  def getDropByUID(dropUID: DropUID, date: Option[DateTime] = None, configuration: Map[String, String] = Map()): Option[WaterfallDrop[_ <: AnyRef, _ <: AnyRef]]
}