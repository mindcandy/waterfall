package com.mindcandy.waterfall

trait IOConfig {
  def url: String
}

trait IOBase {
  def config: IOConfig
}

trait IOSource[A] extends IOBase {
  def retrieveInto[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]): Unit
}

trait IOSink[A] extends IOBase {
  def storeFrom[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]): Unit
}
