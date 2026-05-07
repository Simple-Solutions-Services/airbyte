/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.kafka.format;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.OptionalLong;
import org.apache.kafka.common.TopicPartition;

final class KafkaOffsetState {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String OFFSETS_FIELD = "offsets";

  private final Map<TopicPartition, Long> offsets;

  private KafkaOffsetState(final Map<TopicPartition, Long> offsets) {
    this.offsets = offsets;
  }

  static KafkaOffsetState empty() {
    return new KafkaOffsetState(new HashMap<>());
  }

  static KafkaOffsetState fromJson(final JsonNode state) {
    final KafkaOffsetState offsetState = empty();
    parseStateNode(state, offsetState);
    return offsetState;
  }

  OptionalLong getOffset(final TopicPartition topicPartition) {
    final Long offset = offsets.get(topicPartition);
    return offset == null ? OptionalLong.empty() : OptionalLong.of(offset);
  }

  void put(final TopicPartition topicPartition, final long nextOffset) {
    if (nextOffset < 0) {
      throw new IllegalArgumentException("Kafka offset cannot be negative.");
    }
    offsets.put(topicPartition, nextOffset);
  }

  boolean isEmpty() {
    return offsets.isEmpty();
  }

  JsonNode toJson() {
    final ObjectNode root = MAPPER.createObjectNode();
    final ObjectNode offsetsNode = root.putObject(OFFSETS_FIELD);

    offsets.entrySet().stream()
        .sorted(Comparator
            .comparing((Map.Entry<TopicPartition, Long> entry) -> entry.getKey().topic())
            .thenComparingInt(entry -> entry.getKey().partition()))
        .forEach(entry -> {
          final TopicPartition topicPartition = entry.getKey();
          final ObjectNode topicNode = offsetsNode.has(topicPartition.topic())
              ? (ObjectNode) offsetsNode.get(topicPartition.topic())
              : offsetsNode.putObject(topicPartition.topic());
          topicNode.put(String.valueOf(topicPartition.partition()), entry.getValue());
        });

    return root;
  }

  private static void parseStateNode(final JsonNode node, final KafkaOffsetState offsetState) {
    if (node == null || node.isNull() || node.isMissingNode()) {
      return;
    }

    if (node.isArray()) {
      node.forEach(element -> parseStateNode(element, offsetState));
      return;
    }

    if (node.has(OFFSETS_FIELD)) {
      parseOffsets(node.get(OFFSETS_FIELD), offsetState);
    }
    if (node.has("data")) {
      parseStateNode(node.get("data"), offsetState);
    }
    if (node.has("state")) {
      parseStateNode(node.get("state"), offsetState);
    }
    if (node.has("stream")) {
      final JsonNode stream = node.get("stream");
      parseStateNode(stream.get("stream_state"), offsetState);
      parseStateNode(stream.get("streamState"), offsetState);
    }
    if (node.has("global")) {
      final JsonNode global = node.get("global");
      parseStateNode(global.get("shared_state"), offsetState);
      parseStateNode(global.get("sharedState"), offsetState);
    }
  }

  private static void parseOffsets(final JsonNode offsetsNode, final KafkaOffsetState offsetState) {
    if (offsetsNode == null || !offsetsNode.isObject()) {
      throw new IllegalArgumentException("Kafka state offsets must be an object.");
    }

    final Iterator<Map.Entry<String, JsonNode>> topics = offsetsNode.fields();
    while (topics.hasNext()) {
      final Map.Entry<String, JsonNode> topicEntry = topics.next();
      final JsonNode partitionsNode = topicEntry.getValue();
      if (!partitionsNode.isObject()) {
        throw new IllegalArgumentException("Kafka state partitions must be an object.");
      }

      final Iterator<Map.Entry<String, JsonNode>> partitions = partitionsNode.fields();
      while (partitions.hasNext()) {
        final Map.Entry<String, JsonNode> partitionEntry = partitions.next();
        final int partition = parsePartition(partitionEntry.getKey());
        final long offset = parseOffset(partitionEntry.getValue());
        offsetState.put(new TopicPartition(topicEntry.getKey(), partition), offset);
      }
    }
  }

  private static int parsePartition(final String partition) {
    try {
      return Integer.parseInt(partition);
    } catch (final NumberFormatException e) {
      throw new IllegalArgumentException("Kafka state partition must be an integer: " + partition, e);
    }
  }

  private static long parseOffset(final JsonNode offsetNode) {
    if (offsetNode == null || !offsetNode.canConvertToLong()) {
      throw new IllegalArgumentException("Kafka state offset must be a long.");
    }
    final long offset = offsetNode.asLong();
    if (offset < 0) {
      throw new IllegalArgumentException("Kafka state offset cannot be negative.");
    }
    return offset;
  }

}
