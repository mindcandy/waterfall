package com.mindcandy.waterfall

import java.nio.file.Files
import com.typesafe.scalalogging.slf4j.Logging

trait WaterfallDrop[A, B] extends Logging {
  def source: IOSource[A]
  def sourceIntermediate: Intermediate[A]
  
  def sink: IOSink[B]
  def sinkIntermediate: Intermediate[B]
  
  def transform(): Unit
  
  def run(implicit formatSource: IntermediateFormat[A], formatSink: IntermediateFormat[B]): Unit = {
    source.retrieveInto(sourceIntermediate)
    transform()
    sink.storeFrom(sinkIntermediate)
  }
  
  def newTempFileUrl() = {
    val file = Files.createTempFile("waterfall-drops-", ".tsv")
    file.toFile.deleteOnExit()
    file.toUri.toString
  }
  
  def handleErrors(exceptions: List[Throwable]) = {
    exceptions match {
      case Nil => throw new Exception("Unknown error during drop transform")
      case (head :: tail) => { 
        tail.foreach(logger.error("Exception during drop transform", _))
        throw new Exception("Exception during drop transform", head)
      }
    }
  }
}

trait PassThroughWaterfallDrop[A] extends WaterfallDrop[A, A] {
  val sharedIntermediate = FileIntermediate[A](newTempFileUrl())
  
  def source: IOSource[A]
  def sourceIntermediate = sharedIntermediate
  
  def sink: IOSink[A]
  def sinkIntermediate = sharedIntermediate
  
  def transform(): Unit = () 
}