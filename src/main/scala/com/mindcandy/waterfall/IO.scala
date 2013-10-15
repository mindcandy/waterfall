package com.mindcandy.waterfall

trait IOConfig {
  def url: String
}

trait IOBase[CONF <: IOConfig] {
  def config: CONF
}

trait IOSource[A, CONF <: IOConfig] extends IOBase[CONF] {
  def retrieveInto[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]): Unit
}

trait IOSink[A, CONF <: IOConfig] extends IOBase[CONF] {
  def storeFrom[I <: Intermediate[A]](intermediate: I)(implicit format: IntermediateFormat[A]): Unit
}
