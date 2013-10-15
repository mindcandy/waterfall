package com.mindcandy.waterfall

trait Transformer[A, SOURCECONF <: IOConfig, SINKCONF <: IOConfig] {
  def source: IOSource[A, SOURCECONF]
  def sourceIntermediate: Intermediate[A]
  
  def sink: IOSink[A, SINKCONF]
  def sinkIntermediate: Intermediate[A]
  
  def transform(): Unit
  
  def run(implicit format: IntermediateFormat[A]): Unit = {
    source.retrieveInto(sourceIntermediate)
    transform()
    sink.storeFrom(sinkIntermediate)
  }
}