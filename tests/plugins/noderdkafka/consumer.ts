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

import agent from '../../../src';

const Kafka = require('node-rdkafka');

agent.start({
  serviceName: 'consumer',
  maxBufferSize: 1000,
});

var Transform = require('stream').Transform;

var stream = Kafka.KafkaConsumer.createReadStream(
  {
    'metadata.broker.list': 'localhost:9092',
    'group.id': 'librd-test',
    'socket.keepalive.enable': true,
    'enable.auto.commit': false,
  },
  {},
  {
    topics: 'sw-topic',
    waitInterval: 0,
    objectMode: false,
  },
);

stream.on('error', function (err: any) {
  if (err) console.log(err);
  process.exit(1);
});

stream.pipe(process.stdout);

stream.on('error', function (err: any) {
  console.log(err);
  process.exit(1);
});

stream.consumer.on('event.error', function (err: any) {
  console.log(err);
});
