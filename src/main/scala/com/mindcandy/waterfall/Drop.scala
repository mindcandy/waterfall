package com.mindcandy.waterfall

import java.nio.file.Files

trait WaterfallDrop[A, B] {
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
    val file = Files.createTempFile("waterfall-", ".tsv")
    file.toFile.deleteOnExit()
    file.toUri.toString
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