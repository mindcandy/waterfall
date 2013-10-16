package com.mindcandy.waterfall

import java.io.File

trait Transformer[A] {
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

trait PassThroughTransformer[A] extends Transformer[A] {
  def sharedIntermediate = FileIntermediate[A](File.createTempFile("waterfall", ".tsv").toURI().toString())
  
  def source: IOSource[A]
  def sourceIntermediate = sharedIntermediate
  
  def sink: IOSink[A]
  def sinkIntermediate = sharedIntermediate
  
  def transform(): Unit = () 
}