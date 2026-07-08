/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.kafka.format;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import io.airbyte.commons.json.Jsons;
import java.util.Set;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

class KafkaOffsetStateTest {

  @Test
  void parsesAndSerializesOffsets() {
    final KafkaOffsetState state = KafkaOffsetState.fromJson(Jsons.deserialize("""
        {
          "offsets": {
            "topic-a": {
              "0": 12,
              "1": 15
            }
          }
        }
        """));

    assertEquals(12, state.getOffset(new TopicPartition("topic-a", 0)).getAsLong());
    assertEquals(15, state.getOffset(new TopicPartition("topic-a", 1)).getAsLong());
    assertFalse(state.getOffset(new TopicPartition("topic-a", 2)).isPresent());

    state.put(new TopicPartition("topic-b", 3), 99);

    assertEquals(99, state.toJson().get("offsets").get("topic-b").get("3").asLong());
  }

  @Test
  void parsesAirbyteStateDataWrapper() {
    final KafkaOffsetState state = KafkaOffsetState.fromJson(Jsons.deserialize("""
        {
          "type": "STATE",
          "state": {
            "type": "LEGACY",
            "data": {
              "offsets": {
                "topic-a": {
                  "0": 42
                }
              }
            }
          }
        }
        """));

    assertEquals(42, state.getOffset(new TopicPartition("topic-a", 0)).getAsLong());
  }

  @Test
  void parsesPerStreamStateList() {
    // Shape the platform passes back when the source emits STREAM state messages.
    final KafkaOffsetState state = KafkaOffsetState.fromJson(Jsons.deserialize("""
        [
          {
            "type": "STREAM",
            "stream": {
              "stream_descriptor": { "name": "topic-a" },
              "stream_state": { "offsets": { "topic-a": { "0": 7 } } }
            }
          },
          {
            "type": "STREAM",
            "stream": {
              "stream_descriptor": { "name": "topic-b" },
              "stream_state": { "offsets": { "topic-b": { "2": 21 } } }
            }
          }
        ]
        """));

    assertEquals(7, state.getOffset(new TopicPartition("topic-a", 0)).getAsLong());
    assertEquals(21, state.getOffset(new TopicPartition("topic-b", 2)).getAsLong());
  }

  @Test
  void serializesSingleTopicStreamState() {
    final KafkaOffsetState state = KafkaOffsetState.empty();
    state.put(new TopicPartition("topic-a", 0), 12);
    state.put(new TopicPartition("topic-b", 1), 34);

    assertEquals(Set.of("topic-a", "topic-b"), state.topics());

    final JsonNode topicA = state.toJson("topic-a");
    assertEquals(12, topicA.get("offsets").get("topic-a").get("0").asLong());
    assertFalse(topicA.get("offsets").has("topic-b"));
  }

  @Test
  void rejectsInvalidOffsets() {
    assertThrows(IllegalArgumentException.class, () -> KafkaOffsetState.fromJson(Jsons.deserialize("""
        {
          "offsets": {
            "topic-a": {
              "0": -1
            }
          }
        }
        """)));
  }

}
