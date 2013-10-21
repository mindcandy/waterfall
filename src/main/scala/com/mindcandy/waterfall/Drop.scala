package com.mindcandy.waterfall

import java.nio.file.Files

trait WaterfallDrop[A] {
  def source: IOSource[A]
  def sourceIntermediate: Intermediate[A]
  
  def sink: IOSink[A]
  def sinkIntermediate: Intermediate[A]
  
  def transform(): Unit
  
  def run(implicit format: IntermediateFormat[A]): Unit = {
    source.retrieveInto(sourceIntermediate)
    transform()
    sink.storeFrom(sinkIntermediate)
  }
}

trait PassThroughWaterfallDrop[A] extends WaterfallDrop[A] {
  val sharedIntermediate = FileIntermediate[A](Files.createTempFile("waterfall-", ".tsv").toUri().toString())
  
  def source: IOSource[A]
  def sourceIntermediate = sharedIntermediate
  
  def sink: IOSink[A]
  def sinkIntermediate = sharedIntermediate
  
  def transform(): Unit = () 
}