// Copyright (C) 2018 Don Kelly <karfai@gmail.com>

// This file is part of Interlibr, a functional component of an
// Internet of Rules (IoR).

// ACKNOWLEDGEMENTS
// Funds: Xalgorithms Foundation
// Collaborators: Don Kelly, Joseph Potvin and Bill Olders.

// This program is free software: you can redistribute it and/or
// modify it under the terms of the GNU Affero General Public License
// as published by the Free Software Foundation, either version 3 of
// the License, or (at your option) any later version.

// This program is distributed in the hope that it will be useful, but
// WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
// Affero General Public License for more details.

// You should have received a copy of the GNU Affero General Public
// License along with this program. If not, see
// <http://www.gnu.org/licenses/>.
package org.xalgorithms

import akka.actor._
import akka.event.Logging
import akka.pattern.gracefulStop
import scala.concurrent._
import scala.concurrent.duration._

import org.xalgorithms.actors.{ ActionsActor, AuditActor, TraceActor }
import org.xalgorithms.streams.AkkaStreams

object Main extends App with AkkaStreams {
  import org.xalgorithms.actors.Triggers
  import org.xalgorithms.actors.Events

  implicit val actor_system = ActorSystem("interlibr-service-execute")
  private val _log = Logging(actor_system, this.getClass())

  val actors = Map(
    "verify_rule_execution" -> Props[ActionsActor]
  ).map { case (name, props) => actor_system.actorOf(props, s"actors_${name}") }

  _log.info("# setting up observing actors")
  Seq(
    Props[AuditActor],
    Props[TraceActor]
  ).foreach { props =>
    val ref = actor_system.actorOf(props)
    actor_system.eventStream.subscribe(ref, classOf[Events.Event])
  }

  _log.info("# setting up consumers")
  actors.foreach { ref => ref ! Triggers.InitializeConsumer() }

  scala.sys.addShutdownHook({
    _log.info("# stopping actors")
    actors.foreach { ref =>
      Await.result(gracefulStop(ref, 2 seconds), 3.seconds)
    }

    println("# shutdown")
    actor_system.terminate()
    Await.result(actor_system.whenTerminated, 10.seconds)
  })
}
