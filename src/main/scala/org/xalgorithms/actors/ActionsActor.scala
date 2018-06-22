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

import scala.util.{ Success, Failure }

import org.xalgorithms.actors.Triggers._
import org.xalgorithms.services.{ AkkaLogger, Mongo, MongoActions }

class ActionsActor extends TopicActor("il.verify.rule_execution") {
  private val _mongo = new Mongo(new AkkaLogger("mongo", log))

  def trigger(tr: Trigger): Unit = tr match {
    case TriggerById(request_id) => {
      log.info(s"TriggerById(${request_id})")
      _mongo.find_one(MongoActions.FindTestRunById(request_id)).onComplete {
        case Success(doc) => {
          println(doc)
        }

        case Failure(th) => {
          println("failed")
        }
      }
    }
  }
}
