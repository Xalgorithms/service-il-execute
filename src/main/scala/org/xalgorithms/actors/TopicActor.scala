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
import akka.kafka.{ ConsumerSettings, Subscriptions }
import akka.kafka.scaladsl.Consumer
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{ Flow, Sink }
import java.io.ByteArrayInputStream
import org.apache.kafka.clients.consumer.{ ConsumerConfig, ConsumerRecord }
import org.apache.kafka.common.serialization.{ StringDeserializer }
import play.api.libs.json._
import scala.util.{ Failure, Success }

import org.xalgorithms.actors.Triggers._
import org.xalgorithms.config.Settings
import org.xalgorithms.streams.AkkaStreams

abstract class TopicActor(topic: String) extends Actor with AkkaStreams with ActorLogging {
  implicit val actor_system = context.system

  val kafka_settings = Settings.Kafka
  val consumer_settings = ConsumerSettings(actor_system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers(kafka_settings("bootstrap_servers"))
    .withGroupId(kafka_settings("group_id"))
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true")

  // TODO: research when committable source would be useful
  // https://github.com/akka/reactive-kafka/blob/master/core/src/main/scala/akka/kafka/scaladsl/Consumer.sca
  private val _source = Consumer.plainSource(consumer_settings, Subscriptions.topics(topic))
  private val _flow_json = Flow[ConsumerRecord[String, String]].map { rec =>
    import Implicits.trigger_reads
    Json.parse(rec.value).validate[Trigger] match {
      case (s: JsSuccess[Trigger]) => s.get
      case (e: JsError) => FailureDecode()
    }
  }

  private val _sink = Sink.actorRefWithAck(self, "STREAM_INIT", "OK", "STREAM_DONE")

  def trigger(tr: Trigger): Unit

  def receive: Receive = {
    case "STREAM_INIT" => {
      log.info("> STREAM_INIT")
      sender() ! "OK"
    }

    case InitializeConsumer() => {
      log.info("> InitializeConsumer")
      _source.via(_flow_json).to(_sink).run()
      sender() ! "OK"
    }

    case FailureDecode() => {
      log.error("! failed message decode")
    }

    case (tr: Trigger) => {
      log.info("> received a trigger")
      trigger(tr)
      sender() ! "OK"
    }

    case other => {
      log.error("> something unconsidered")
      println(other)
      sender() ! "OK"
    }
  }
}
