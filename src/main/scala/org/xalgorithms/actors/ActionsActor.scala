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
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{ Success, Failure }

import org.xalgorithms.actors.Triggers._
import org.xalgorithms.rules._
import org.xalgorithms.rules.elements._
import org.xalgorithms.services.{ AkkaLogger, Documents, Mongo, MongoActions }

class ActionsActor extends TopicActor("il.verify.rule_execution") {
  private val _mongo = new Mongo(new AkkaLogger("mongo", log))

  class LoadFromMongoBsonTableSource extends LoadBsonTableSource {
    private val _cache = scala.collection.mutable.Map[String, BsonArray]()

    private def lookup_table(ptref: PackagedTableReference): BsonArray = {
      val table_props = s"package_name=${ptref.package_name}; id=${ptref.id}; ver=${ptref.version}"
      log.info(s"asked to find table (${table_props})")
      val q = _mongo.find_one(
        MongoActions.FindTableByReference(ptref.package_name, ptref.id, ptref.version))
      try {
        val res = Await.result(q, 5.seconds)
        Documents.maybe_find_array(res, "table").getOrElse(new BsonArray())
      } catch {
        case (th: java.util.concurrent.TimeoutException) => {
          log.error(s"connection timed out looking for table, yielding empty (${table_props})")
          new BsonArray()
        }
      }
    }

    private def maybe_find_in_cache(ptref: PackagedTableReference): BsonArray = {
      val k = s"${ptref.package_name}/${ptref.id}/${ptref.version}"
      _cache.get(k) match {
        case Some(a) => {
          log.info("reading table from cache")
          a
        }
        case None => {
          log.info("serving table from cache")
          val a = lookup_table(ptref)
          _cache.put(k, a)
          a
        }
      }
    }

    def read(ptref: PackagedTableReference): BsonArray = {
      maybe_find_in_cache(ptref)
    }
  }

  def build_context(opt_ctx_doc: Option[BsonDocument]): Context = {
    val ctx = new GlobalContext(new LoadFromMongoBsonTableSource())
    // TODO: add opt_ctx_doc
    ctx
  }

  def execute_one(rule_id: String, opt_ctx_doc: Option[BsonDocument]): Unit = {
    log.info(s"executing rule (rule_id=${rule_id})")
    _mongo.find_one(MongoActions.FindRuleById(rule_id)).onComplete {
      case Success(rule_doc) => {
        log.debug("building syntax")
        val steps = SyntaxFromBson(rule_doc)
        log.debug("building ctx")
        val ctx = build_context(opt_ctx_doc)
        log.info("executing steps")
        steps.foreach { step =>
          step.execute(ctx)
        }
        log.info("executed all steps")
      }

      case Failure(th) => {
        log.error("did not find the rule doc");
      }
    }
  }

  def trigger(tr: Trigger): Unit = tr match {
    case TriggerById(request_id) => {
      log.info(s"TriggerById(${request_id})")
      _mongo.find_one(MongoActions.FindTestRunById(request_id)).onComplete {
        case Success(doc) => {
          Documents.maybe_find_text(doc, "rule_id") match {
            case Some(rule_id) => {
              execute_one(rule_id, Documents.maybe_find_document(doc, "context"))
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
