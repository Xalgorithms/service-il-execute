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

import akka.actor.{ ActorSystem, Props }
import scala.concurrent.{ Await }
import scala.concurrent.duration._

import org.xalgorithms.actors._
import org.xalgorithms.config.{ Settings }
import org.xalgorithms.streams.{ ConsumerStreams }

object Main extends App with ConsumerStreams {
  implicit val actor_system = ActorSystem("interlibr-service-execute")

  val actor_action_stream = actor_system.actorOf(Props(new ActionStream), "action_stream")

  println(s"# setting up consumer (${actor_action_stream})")

  val settings = Settings(actor_system).Kafka
  val src = make_source(settings)
  val sink = make_sink(actor_action_stream)
  val flow = make_flow()
  val stream = src.via(flow).to(sink).run()
//  val stream = src.to(sink).run()

  scala.sys.addShutdownHook({
    println("# shutdown")
    actor_system.terminate()
    Await.result(actor_system.whenTerminated, 10.seconds)
  })
}
