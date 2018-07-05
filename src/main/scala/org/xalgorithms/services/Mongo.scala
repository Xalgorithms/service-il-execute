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
package org.xalgorithms.services

import java.security.MessageDigest
import java.util.UUID.randomUUID
import com.mongodb.client.result.UpdateResult
import org.mongodb.scala.bson.{ BsonDocument, BsonArray }
import org.mongodb.scala.bson.conversions.Bson
import org.mongodb.scala._
import org.mongodb.scala.model.Aggregates._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Projections._
import org.mongodb.scala.model.Sorts._
import org.mongodb.scala.model.Updates._
import org.mongodb.scala.model._
import play.api.libs.Codecs
import play.api.libs.json.JsValue
import scala.concurrent.{ Future, Promise }

// FIXME: should use an actor's execution context
import scala.concurrent.ExecutionContext.Implicits.global

object MongoActions {
  abstract class Store() {
    def document : Document
    def id: String
  }

  case class StoreDocument(doc: BsonDocument) extends Store {
    private val public_id = randomUUID.toString()

    def document: Document = {
      Document("public_id" -> public_id, "content" -> doc)
    }

    def id = public_id
  }

  case class StoreTestRun(rule_id: String, ctx: BsonDocument) extends Store {
    private val request_id = randomUUID.toString()

    def document: Document = {
      Document("rule_id" -> rule_id, "request_id" -> request_id, "context" -> ctx)
    }

    def id = request_id
  }

  case class StoreTrace(request_id: String) extends Store {
    private val public_id = randomUUID.toString()

    def document: Document = Document(
      "public_id"  -> public_id,
      "request_id" -> request_id,
      "steps" -> BsonArray()
    )

    def id = public_id
  }

  abstract class Update {
    def collection: String
    def condition: Unit
    def update: Bson
  }

  case class AddContext(public_id: String, phase: String, index: Int, ctx: JsValue) extends Update {
    def collection = "traces"
    def condition = equal("public_id", public_id)
    def update = push("steps", Document(ctx.toString))
  }

  class FindOne()
  case class FindByKey(cn: String, key: String, value: String) extends FindOne

  object FindDocumentById {
    def apply(id: String) = FindByKey("documents", "public_id", id)
  }

  object FindExecutionById {
    def apply(id: String) = FindByKey("executions", "request_id", id)
  }

  object FindRuleById {
    def apply(id: String) = FindByKey("rules", "public_id", id)
  }

  object FindTableByReference {
    def apply(pkg: String, name: String, ver: String) = {
      val s = s"T(${pkg}:${name}:${ver})"
      val k = play.api.libs.Codecs.sha1(s.getBytes)

      FindByKey("tables", "public_id", k)
    }
  }
}

class Mongo(log: Logger = new LocalLogger) {
  val url = sys.env.get("MONGO_URL").getOrElse("mongodb://127.0.0.1:27017/")
  val cl = MongoClient(url)
  val db = cl.getDatabase("xadf")

  def find_one(op: MongoActions.FindOne): Future[BsonDocument] = {
    val pr = Promise[BsonDocument]()

    op match {
      case MongoActions.FindByKey(cn, key, value) => {
        db.getCollection(cn).find(equal(key, value)).first().subscribe(
          (doc: Document) => pr.success(doc.toBsonDocument)
        )
      }
    }

    pr.future
  }

  def store(op: MongoActions.Store): Future[String] = {
    val pr = Promise[String]()
    val cn = op match {
      case MongoActions.StoreDocument(_) => "documents"
      case MongoActions.StoreTestRun(_, _)  => "test-runs"
      case MongoActions.StoreTrace(_)  => "traces"
    }

    db.getCollection(cn).insertOne(op.document).subscribe(new Observer[Completed] {
      override def onComplete(): Unit = {
        log.debug(s"insert completed")
      }

      override def onNext(res: Completed): Unit = {
        log.debug(s"insert next")
        pr.success(op.id)
      }

      override def onError(th: Throwable): Unit = {
        log.error(s"failed insert, trigging promise")
        pr.failure(th)
      }
    })

    pr.future
  }

  def update(op: MongoActions.Update): Future[Unit] = {
    val pr = Promise[Unit]()
    op match {
      case MongoActions.AddContext(request_id, phase, index, ctx) => {
        val doc = Document(
          "index" -> index,
          "phase" -> phase,
          "context" -> Document(ctx.toString))
        db.getCollection("traces").updateOne(
          equal("request_id", request_id),
          push("steps", doc)
        ).subscribe((update: UpdateResult) => {
          log.info(s"updated (request_id=${request_id}; matched=${update.getMatchedCount}; modified=${update.getModifiedCount})")
          pr.success(None)
        })
      }
    }

    pr.future
  }
}
