package com.mindcandy.waterfall.io

trait IOConfig {
  def url: String
}

trait IOBase[CONF <: IOConfig] {
  def config: CONF
}

trait IOSource[A, CONF <: IOConfig] extends IOBase[CONF] {
  def retrieveInto[I <: Intermediate[A]](intermediate: I)(implicit toConverter: (Seq[String] => A), fromConverter: (A => Seq[String])): Unit
}

trait IOSink[A, CONF <: IOConfig] extends IOBase[CONF] {
  def storeInto[I <: Intermediate[A]](intermediate: I)(implicit toConverter: (Seq[String] => A), fromConverter: (A => Seq[String])): Unit
}
