/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.kafka.format;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.airbyte.commons.json.Jsons;
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
