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

import org.bson._
import scala.collection.JavaConverters._
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{ Success, Failure }

import org.xalgorithms.actors.Triggers._
import org.xalgorithms.rules._
import org.xalgorithms.rules.elements._
import org.xalgorithms.services.{ AkkaLogger, Documents, Mongo, MongoActions }

class ActionsActor extends TopicActor("il.verify.rule_execution") {
  private val _mongo = new Mongo(new AkkaLogger("mongo", log))

  def execute_one(request_id: String, rule_id: String, opt_ctx_doc: Option[BsonDocument]): Unit = {
    log.info(s"executing rule (rule_id=${rule_id})")
    _mongo.find_one(MongoActions.FindRuleById(rule_id)).onComplete {
      case Success(rule_doc) => {
        log.debug("building syntax")
        val steps = SyntaxFromBson(rule_doc)
        log.debug("building ctx")
        val ctx = ExecutionContext(new AkkaLogger("exec ctx", log), _mongo, opt_ctx_doc)
        log.info("executing steps")
        context.system.eventStream.publish(Events.ExecutionStarted(request_id))
        steps.zipWithIndex.foreach { case (step, i) =>
          log.info(s"starting step (i=${i})")
          context.system.eventStream.publish(Events.StepStarted(request_id, i, ctx.serialize))
          step.execute(ctx)
          context.system.eventStream.publish(Events.StepFinished(request_id, i, ctx.serialize))
          log.info(s"finished step (i=${i})")
        }
        log.info("executed all steps")
        context.system.eventStream.publish(Events.ExecutionFinished(request_id))
      }

      case Failure(th) => {
        log.error("did not find the rule doc");
      }
    }
  }

  def trigger(tr: Trigger): Unit = tr match {
    case TriggerById(request_id) => {
      log.info(s"TriggerById(${request_id})")
      _mongo.find_one(MongoActions.FindExecutionById(request_id)).onComplete {
        case Success(doc) => {
          log.info("found related document")
          Documents.maybe_find_text(doc, "rule_id") match {
            case Some(rule_id) => {
              log.info(s"executing rule (${rule_id})")
              execute_one(request_id, rule_id, Documents.maybe_find_document(doc, "context"))
            }
            case None => log.error("failed to find rule_id")
          }
        }

        case Failure(th) => {
          log.error("failed to find document")
        }
      }
    }
  }
}
