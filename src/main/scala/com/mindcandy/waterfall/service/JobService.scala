package com.mindcandy.waterfall.service

import spray.routing.HttpService
import com.typesafe.scalalogging.slf4j.Logging
import akka.actor.Actor
import argonaut._
import Argonaut._
import akka.actor.Props
import akka.actor.ActorRef
import com.mindcandy.waterfall.actors.Protocol._
import spray.httpx.marshalling.MetaMarshallers.optionMarshaller
import com.mindcandy.waterfall.actors.JobDatabaseActor.GetJob

object JobServiceActor {
  def props(jobDatabase: ActorRef): Props = Props(new JobServiceActor(jobDatabase))
}

class JobServiceActor(val jobDatabase: ActorRef) extends Actor with JobService {
  def actorRefFactory = context
  def receive = runRoute(route)
}

trait JobService extends HttpService with ArgonautMarshallers {
  def jobDatabase: ActorRef
  
  val route = path("jobs" / IntNumber) { id =>
    get { 
      produce(instanceOf[Option[DropJob]]) { completionFunction => context =>
        jobDatabase ! GetJob(id, completionFunction)
      }
    }
  }
}