package com.mindcandy.waterfall.actor

import akka.actor.{ActorContext, ActorRef}

object TestDropWorkerFactory {
  def apply(actor: ActorRef) = new TestDropWorkerFactory(actor)
}

class TestDropWorkerFactory(actor: ActorRef) extends DropWorkerFactory {
  override def createWorker(implicit context: ActorContext): ActorRef = actor
}
