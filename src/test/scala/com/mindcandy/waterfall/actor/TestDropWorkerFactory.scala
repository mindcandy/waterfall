package com.mindcandy.waterfall.actor

import akka.actor.{ ActorContext, ActorRef, Props }

object TestDropWorkerFactory {
  def apply(actor: ActorRef) = new TestDropWorkerFactory(actor)
}

class TestDropWorkerFactory(actor: ActorRef) extends ActorFactory {
  def props = Props()
  override def createActor(implicit context: ActorContext): ActorRef = actor
}
