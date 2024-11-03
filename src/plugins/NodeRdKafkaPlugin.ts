/*!
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

import SwPlugin from '../core/SwPlugin';
import PluginInstaller from '../core/PluginInstaller';
import ContextManager from '../trace/context/ContextManager';
import { Component } from '../trace/Component';
import { SpanLayer } from '../proto/language-agent/Tracing_pb';
import Tag from '../Tag';

class NodeRdKafkaPlugin implements SwPlugin {
  readonly module = 'node-rdkafka';
  readonly versions = '*';

  install(installer: PluginInstaller): void {
    const kafka = installer.require?.('node-rdkafka') ?? require('node-rdkafka');
    console.log('my|install Producer = ', kafka.Producer);
    this.interceptProducerSend(kafka.Producer);
  }

  interceptProducerSend(origProducer: any): void {
    const _produce = origProducer.prototype.produce;

    origProducer.prototype.produce = function (
      topic: any,
      partition: any,
      message: any,
      key: any,
      timestamp: any,
      opaque: any,
      headers: any,
    ) {
      const brokers = this.globalConfig['metadata.broker.list'] || '';
      const span = ContextManager.current.newExitSpan('Kafka/' + topic + '/Producer', Component.KAFKA_PRODUCER);
      span.start();

      try {
        span.inject().items.forEach((item) => headers.push({ [item.key]: item.value }));
        console.log('my|interceptProducer header = ', headers);
        span.component = Component.KAFKA_PRODUCER;
        span.layer = SpanLayer.MQ;
        span.peer = brokers;
        span.tag(Tag.mqTopic(topic));

        const ret = _produce.call(this, topic, partition, message, key, timestamp, opaque, headers);
        span.stop();

        return ret;
      } catch (e) {
        span.error(e);
        span.stop();

        throw e;
      }
    };
  }
}

export default new NodeRdKafkaPlugin();
