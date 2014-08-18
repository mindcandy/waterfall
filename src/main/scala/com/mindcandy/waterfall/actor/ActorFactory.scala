package com.mindcandy.waterfall.actor

import akka.actor.{ ActorContext, ActorRef, Props }

trait ActorFactory {
  def props: Props
  def createActor(implicit context: ActorContext): ActorRef = context.actorOf(props)
}
