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

import akka.actor._
import akka.stream.{ ActorMaterializer }
import scala.util.{ Success, Failure }

// ours
import org.xalgorithms.storage.data.{ Mongo, MongoActions }

// local
import org.xalgorithms.services.{ AkkaLogger }

class TraceActor extends Actor with ActorLogging {
  implicit val materializer = ActorMaterializer()
  implicit val execution_context = context.dispatcher

  private val _mongo = new Mongo(new AkkaLogger("mongo", log))

  def receive = {
    case Events.ExecutionStarted(id) => {
      log.info(s"started executing (id=${id})")
      _mongo.store(MongoActions.StoreTrace(id)).onComplete {
        case Success(id) => {
          log.info(s"stored trace (public_id=${id})")
        }
        case Failure(th) => {
          log.error(s"failed to store trace for request (id=${id})")
        }
      }
    }

    case Events.ExecutionFinished(id) => {
      log.info(s"finished executing (id=${id})")
    }

    case Events.StepStarted(id, index, ctx) => {
      log.info(s"step started (${index})")
      _mongo.update(MongoActions.AddContext(id, "start", index, ctx)).onComplete {
        case Success(o) => log.info(s"added context (id=${id})")
        case Failure(th) => log.error(s"failed to add context (id=${id}; th=${th.getMessage})")
      }
    }

    case Events.StepFinished(id, index, ctx) => {
      log.info(s"step finished (${index})")
      _mongo.update(MongoActions.AddContext(id, "finish", index, ctx)).onComplete {
        case Success(o) => log.info(s"added context (id=${id})")
        case Failure(th) => log.error(s"failed to add context (id=${id}; th=${th.getMessage})")
      }
    }
  }
}
