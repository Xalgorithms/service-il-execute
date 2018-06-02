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
package org.xalgorithms.actors

import akka.actor.{ Actor, ActorRef }

import org.xalgorithms.streams.{ ConsumerStreams }
import org.xalgorithms.config.{ Settings }

case class InitializeStream(actor_ref: ActorRef)
case class TerminateStream(topic: String)

// ControlStreams??? StreamControl???
class ConsumerStream extends Actor with ConsumerStreams {
  implicit val actor_system = context.system

  val settings = Settings(actor_system).Kafka

  def receive(): Receive = {
    case InitializeStream(actor_ref) => {
      println(s"# initialize (actor_ref=${actor_ref})")
      val source = make_source(settings)
      // flow???
      val sink = make_sink(actor_ref)
      val stream = source.to(sink).run()

      actor_ref ! Triggers.ActivatedStream(settings("topic"))
      //publishLocalEvent(Triggers.ActivatedStream(props("topic")))
    }

    case "OK" => {
      println("< OK")
    }

    case TerminateStream(topic) => {

    }
  }
}
