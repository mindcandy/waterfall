package com.mindcandy.waterfall

import java.nio.file.Files
import com.typesafe.scalalogging.slf4j.Logging
import scala.util.Try
import scala.util.Success

trait WaterfallDrop[A, B] extends Logging {
  def source: IOSource[A]
  def sourceIntermediate: Intermediate[A]
  
  def sink: IOSink[B]
  def sinkIntermediate: Intermediate[B]
  
  def transform(sourceInter: Intermediate[A], sinkInter: Intermediate[B]): Try[Unit]
  
  def run(implicit formatSource: IntermediateFormat[A], formatSink: IntermediateFormat[B]): Try[Unit] = {
    for {
      _ <- source.retrieveInto(sourceIntermediate)
      _ <- transform(sourceIntermediate, sinkIntermediate)
      _ <- sink.storeFrom(sinkIntermediate)
    } yield ()
  }
}

trait PassThroughWaterfallDrop[A] extends WaterfallDrop[A, A] {
  def fileUrl: String
  val sharedIntermediate = FileIntermediate[A](fileUrl)
  
  def source: IOSource[A]
  def sourceIntermediate = sharedIntermediate
  
  def sink: IOSink[A]
  def sinkIntermediate = sharedIntermediate
  
  def transform(sourceInter: Intermediate[A], sinkInter: Intermediate[A]): Try[Unit] = Success(()) 
}