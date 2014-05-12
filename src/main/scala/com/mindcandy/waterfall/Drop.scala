package com.mindcandy.waterfall

import java.nio.file.Files
import com.typesafe.scalalogging.slf4j.Logging
import scala.util.Try
import scala.util.Success

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

trait PassThroughWaterfallDrop[A] extends WaterfallDrop[A, A] {
  def fileUrl: String
  val sharedIntermediate = FileIntermediate[A](fileUrl)
  def sharedIntermediateFormat: IntermediateFormat[A]
  
  def source: IOSource[A]
  def sourceIntermediate = sharedIntermediate
  def sourceIntermediateFormat = sharedIntermediateFormat
  
  def sink: IOSink[A]
  def sinkIntermediate = sharedIntermediate
  def sinkIntermediateFormat = sharedIntermediateFormat
  
  def transform(sourceInter: Intermediate[A], sinkInter: Intermediate[A]): Try[Unit] = Success(()) 
}

object WaterfallDropFactory {
  type DropUID = String
}

trait WaterfallDropFactory {
  import WaterfallDropFactory._
  
  def getDropByUID(dropUID: DropUID): Option[WaterfallDrop[_, _]]
}