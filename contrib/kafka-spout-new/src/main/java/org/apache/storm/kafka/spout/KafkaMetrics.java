//  Copyright 2016 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License
package org.apache.storm.kafka.spout;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.task.TopologyContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaMetrics implements IMetric {
  private final KafkaConsumer<?, ?> consumer;
  private final String nodeMetricsGroup = "consumer-node-metrics";
  private final String topicMetricsGroup = "consumer-topic-metrics";
  private static final Logger LOG = LoggerFactory.getLogger(KafkaSpout.class);

  private KafkaMetrics(KafkaConsumer<?, ?> consumer) {
    this.consumer = consumer;
  }

  public static void register(TopologyContext context, KafkaConsumer<?, ?> aConsumer) {
    context.registerMetric("kafka", new KafkaMetrics(aConsumer), 60);
  }

  @Override
  public Object getValueAndReset() {
    Map<MetricName, ? extends Metric> metrics = consumer.metrics();
    Map<String, Double> translatedMetrics = new HashMap<>();

    for (Map.Entry<MetricName, ? extends Metric> entry : metrics.entrySet()) {
      try {
        MetricName name = entry.getKey();
        String group = name.group();
        String scope = null;
        if (group.equals(nodeMetricsGroup)) {
          scope = name.tags().get("node-id").substring(5);
          if (scope.equals("-1")) {
            scope = null;
          }
        } else if (group.equals(topicMetricsGroup)) {
          scope = name.tags().get("topic");
        }

        String fullName;
        if (scope != null) {
          fullName = String.join("/", group, scope, name.name());
        } else {
          fullName = String.join("/", group, name.name());
        }
        translatedMetrics.put(fullName, entry.getValue().value());
      }
      catch (Exception e) {
        LOG.error("Error publishing metric {}", entry.getKey(), e);
      }
    }

    return translatedMetrics;
  }
}
