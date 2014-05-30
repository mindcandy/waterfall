package com.mindcandy.waterfall.drop

import com.typesafe.scalalogging.slf4j.Logging
import scala.util.Try
import scala.util.Success
import com.mindcandy.waterfall._
import scala.util.Success
import org.joda.time.DateTime

trait WaterfallDrop[A, B] extends Logging {
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
  import WaterfallDropFactory._
  
  def getDropByUID(dropUID: DropUID, date: Option[DateTime]=None): Option[WaterfallDrop[_, _]]
}