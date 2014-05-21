package com.mindcandy.waterfall.actor

import akka.actor.{ActorRef, ActorContext, Props}

trait ActorFactory {
  def props: Props
  def createActor(implicit context: ActorContext): ActorRef = context.actorOf(props)
}
