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

import play.api.libs.json._

object Triggers {
  case class InitializeConsumer()
  case class FailureDecode()

  abstract class Trigger
  case class TriggerById(id: String) extends Trigger
}

object Events {
  abstract class Event
  case class ExecutionStarted(id: String) extends Event
  case class ExecutionFinished(id: String) extends Event
  case class StepStarted(id: String, number: Int, context: JsValue) extends Event
  case class StepFinished(id: String, number: Int, context: JsValue) extends Event
}

object Implicits {
  import Triggers._

  implicit val trigger_reads = new Reads[Trigger] {
    def reads(json: JsValue): JsResult[Trigger] = {
      val opt_tr = (json \ "context" \ "action").as[String] match {
        case "trigger_by_id" => (json \ "args" \ "id").asOpt[String].map { id => new TriggerById(id) }
        case _ => None
      }

      opt_tr match {
        case Some(tr) => JsSuccess(tr)
        case None     => JsError()
      }
    }
  }
}
