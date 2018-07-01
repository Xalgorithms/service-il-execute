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
import play.api.libs.json._
import java.io.{ OutputStream }

abstract class Audit
case class AuditExecutionStart(request_id: String) extends Audit
case class AuditExecutionEnd(request_id: String) extends Audit

class AuditSerializer extends Serializer[Audit] {
  implicit val aes_writes = new Writes[AuditExecutionStart] {
    def writes(aes: AuditExecutionStart) = Json.obj(
      "context" -> Map("task" -> "execution", "action" -> "start"),
      "args" -> Map("request_id" -> aes.request_id)
    )
  }

  implicit val aee_writes = new Writes[AuditExecutionEnd] {
    def writes(aee: AuditExecutionEnd) = Json.obj(
      "context" -> Map("task" -> "execution", "action" -> "end"),
      "args" -> Map("request_id" -> aee.request_id)
    )
  }

  def write(o: Audit): String = {
    (o match {
      case (aes: AuditExecutionStart) => Json.toJson(aes)
      case (aee: AuditExecutionEnd) => Json.toJson(aee)
      case _ => ""
    }).toString
  }
}

class AuditActor extends Actor with ActorLogging with QueueForKafka[Audit] {
  implicit val actor_system = context.system
  implicit val topic = "il.emit.audit"
  implicit val serializer = new AuditSerializer()

  def receive = {
    case Events.ExecutionStarted(id) => {
      log.info(s"started executing (id=${id})")
      send(AuditExecutionStart(id))
    }

    case Events.ExecutionFinished(id) => {
      log.info(s"finished executing (id=${id})")
      send(AuditExecutionEnd(id))
    }
  }
}
