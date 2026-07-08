/*
 * Copyright (c) 2026 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.kafka.format;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.integrations.source.kafka.KafkaProtocol;
import io.airbyte.protocol.models.v0.AirbyteMessage;
import io.airbyte.protocol.models.v0.AirbyteStateMessage;
import io.airbyte.protocol.models.v0.AirbyteStreamState;
import io.airbyte.protocol.models.v0.StreamDescriptor;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule;
import org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.msk.auth.iam.IAMClientCallbackHandler;

public abstract class AbstractFormat implements KafkaFormat {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractFormat.class);

  /**
   * Number of records between two emitted STATE checkpoints. Coarser checkpoints reduce
   * platform-side state churn (vs. one STATE per record); on failure the connector re-reads from
   * the last persisted checkpoint, so the at-least-once guarantee is unaffected.
   */
  private static final int STATE_CHECKPOINT_INTERVAL = 1000;

  protected Set<String> topicsToSubscribe;
  protected JsonNode config;
  private KafkaOffsetState offsetState = KafkaOffsetState.empty();
  private boolean manageOffsets;

  public AbstractFormat(JsonNode config) {
    this.config = config;

  }

  protected abstract KafkaConsumer<String, ?> getConsumer();

  protected abstract Set<String> getTopicsToSubscribe();

  protected void prepareForRead(final JsonNode state) {
    offsetState = KafkaOffsetState.fromJson(state);
    manageOffsets = true;
    LOGGER.info("Prepared Kafka read with Airbyte offset state: {}", offsetState.toJson());
    LOGGER.info("Kafka broker auto-commit is disabled; Airbyte STREAM state messages are the checkpoint source of truth, mirrored to the broker on start.");
  }

  protected boolean shouldManageOffsets() {
    return manageOffsets;
  }

  protected ConsumerRebalanceListener statefulRebalanceListener(final KafkaConsumer<?, ?> consumer) {
    return new ConsumerRebalanceListener() {

      @Override
      public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
        LOGGER.debug("Kafka partitions revoked: {}", partitions);
      }

      @Override
      public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
        LOGGER.debug("Kafka partitions assigned: {}", partitions);
        seekToStateOrConfiguredStart(consumer, partitions);
      }

    };
  }

  protected void seekToStateOrConfiguredStart(final KafkaConsumer<?, ?> consumer,
                                              final Collection<TopicPartition> partitions) {
    if (!shouldManageOffsets() || partitions.isEmpty()) {
      return;
    }

    final List<TopicPartition> seekToBeginning = new ArrayList<>();
    final List<TopicPartition> seekToEnd = new ArrayList<>();
    final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
    LOGGER.info("Seeking assigned Kafka partitions using Airbyte state. partitions={}, auto_offset_reset={}, state={}",
        partitions, getAutoOffsetReset(), offsetState.toJson());
    for (final TopicPartition partition : partitions) {
      final OptionalLong offset = offsetState.getOffset(partition);
      if (offset.isPresent()) {
        consumer.seek(partition, offset.getAsLong());
        offsetsToCommit.put(partition, new OffsetAndMetadata(offset.getAsLong()));
        LOGGER.info("Seeking Kafka partition {} to Airbyte state offset {}.", partition, offset.getAsLong());
      } else {
        switch (getAutoOffsetReset()) {
          case "earliest" -> {
            seekToBeginning.add(partition);
            LOGGER.info("No Airbyte state for Kafka partition {}; seeking to beginning.", partition);
          }
          case "latest" -> {
            seekToEnd.add(partition);
            LOGGER.info("No Airbyte state for Kafka partition {}; seeking to end.", partition);
          }
          case "none" -> throw new IllegalStateException(
              "No Airbyte state for Kafka partition " + partition + " and auto_offset_reset is none.");
          default -> throw new IllegalArgumentException("Unsupported auto_offset_reset value: " + getAutoOffsetReset());
        }
      }
    }

    if (!seekToBeginning.isEmpty()) {
      consumer.seekToBeginning(seekToBeginning);
    }
    if (!seekToEnd.isEmpty()) {
      consumer.seekToEnd(seekToEnd);
    }

    commitStateOffsetsToBroker(consumer, offsetsToCommit);
  }

  /**
   * Mirrors the offsets carried in the incoming Airbyte state back to the Kafka broker as
   * consumer-group commits, so broker-side tooling (e.g. Kafdrop) reflects the connector's position.
   *
   * <p>This is intentionally driven only by the <em>incoming</em> state — i.e. data already
   * confirmed delivered by Airbyte in a prior sync — and never by offsets accumulated mid-run.
   * That keeps the at-least-once guarantee intact: the Airbyte STATE remains the source of truth,
   * and the broker commit is a best-effort, observability-only mirror. A commit failure is logged
   * and swallowed rather than failing the sync.
   */
  private void commitStateOffsetsToBroker(final KafkaConsumer<?, ?> consumer,
                                          final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit) {
    if (offsetsToCommit.isEmpty()) {
      return;
    }
    if (!hasGroupId()) {
      LOGGER.warn("Skipping Kafka broker offset commit: no group_id configured. partitions={}", offsetsToCommit.keySet());
      return;
    }
    try {
      consumer.commitSync(offsetsToCommit);
      LOGGER.info("Committed Airbyte state offsets back to Kafka broker. offsets={}", offsetsToCommit);
    } catch (final RuntimeException e) {
      LOGGER.warn("Failed to commit Airbyte state offsets to Kafka broker; continuing with Airbyte state as source of truth. offsets={}",
          offsetsToCommit, e);
    }
  }

  private boolean hasGroupId() {
    return config.has("group_id") && !config.get("group_id").asText().isBlank();
  }

  protected <T> AutoCloseableIterator<AirbyteMessage> readRecords(final KafkaConsumer<String, T> consumer,
                                                                  final Function<ConsumerRecord<String, T>, AirbyteMessage> messageFactory) {
    final int retry = config.has("repeated_calls") ? config.get("repeated_calls").intValue() : 0;
    final int pollingTime = config.has("polling_time") ? config.get("polling_time").intValue() : 100;
    final int maxRecords = config.has("max_records_process") ? config.get("max_records_process").intValue() : 100000;
    final Map<String, Integer> emptyPollsByTopic = new HashMap<>();
    getTopicsToSubscribe().forEach(topic -> emptyPollsByTopic.put(topic, 0));
    final AtomicBoolean closed = new AtomicBoolean(false);
    LOGGER.info("Starting Kafka read. topics={}, pollingTimeMs={}, repeatedCalls={}, maxRecords={}, initialOffsets={}",
        getTopicsToSubscribe(), pollingTime, retry, maxRecords, offsetState.toJson());

    final Iterator<AirbyteMessage> iterator = new AbstractIterator<>() {

      private Iterator<ConsumerRecord<String, T>> records = Collections.emptyIterator();
      private final Deque<AirbyteMessage> pendingStates = new ArrayDeque<>();
      private final Set<String> topicsSinceCheckpoint = new TreeSet<>();
      private int recordsSinceCheckpoint;
      private boolean readComplete;
      private int recordCount;
      private long pollCount;
      private long receivedCount;

      @Override
      protected AirbyteMessage computeNext() {
        if (!pendingStates.isEmpty()) {
          return pendingStates.poll();
        }

        if (readComplete) {
          return closeAndFinish();
        }

        if (recordCount >= maxRecords) {
          LOGGER.info("Max record count is reached.");
          return completeRead();
        }

        while (!records.hasNext()) {
          final ConsumerRecords<String, T> consumerRecords = consumer.poll(Duration.of(pollingTime, ChronoUnit.MILLIS));
          pollCount++;
          receivedCount += consumerRecords.count();
          LOGGER.debug("Kafka poll completed. pollNumber={}, receivedRecords={}, partitionProgress={}, totalReceivedRecords={}, emittedRecords={}",
              pollCount, consumerRecords.count(), partitionProgress(consumerRecords), receivedCount, recordCount);
          if (consumerRecords.count() == 0) {
            consumer.assignment().stream()
                .map(TopicPartition::topic)
                .distinct()
                .forEach(topic -> emptyPollsByTopic.merge(topic, 1, Integer::sum));

            final boolean complete = emptyPollsByTopic.values().stream().allMatch(emptyPollCount -> emptyPollCount > retry);
            LOGGER.debug("Kafka empty poll progress. pollNumber={}, emptyPollsByTopic={}, complete={}",
                pollCount, emptyPollsByTopic, complete);
            if (complete) {
              LOGGER.info("There is no new data in the queue.");
              return completeRead();
            }
          } else {
            emptyPollsByTopic.replaceAll((topic, emptyPollCount) -> 0);
            records = consumerRecords.iterator();
          }
        }

        final ConsumerRecord<String, T> record = records.next();
        final AirbyteMessage message = messageFactory.apply(record);
        recordCount++;
        offsetState.put(new TopicPartition(record.topic(), record.partition()), record.offset() + 1);
        topicsSinceCheckpoint.add(record.topic());
        recordsSinceCheckpoint++;
        LOGGER.debug("Emitting Kafka record. topic={}, partition={}, offset={}, nextOffset={}, emittedRecords={}",
            record.topic(), record.partition(), record.offset(), record.offset() + 1, recordCount);
        if (recordsSinceCheckpoint >= STATE_CHECKPOINT_INTERVAL) {
          enqueueStateCheckpoint();
        }
        return message;
      }

      /**
       * Queues one STREAM state message per topic that advanced since the previous checkpoint. The
       * states are emitted right after the record that triggered the checkpoint, so a state always
       * follows the records it covers.
       */
      private void enqueueStateCheckpoint() {
        LOGGER.info("Emitting Airbyte STREAM state checkpoint. topics={}, emittedRecords={}, offsets={}",
            topicsSinceCheckpoint, recordCount, offsetState.toJson());
        topicsSinceCheckpoint.forEach(topic -> pendingStates.add(stateMessage(topic)));
        topicsSinceCheckpoint.clear();
        recordsSinceCheckpoint = 0;
      }

      private AirbyteMessage completeRead() {
        readComplete = true;
        if (!topicsSinceCheckpoint.isEmpty()) {
          enqueueStateCheckpoint();
          return pendingStates.poll();
        }
        return closeAndFinish();
      }

      private AirbyteMessage closeAndFinish() {
        LOGGER.info("Kafka read finished. polls={}, receivedRecords={}, emittedRecords={}, finalOffsets={}",
            pollCount, receivedCount, recordCount, offsetState.toJson());
        closeConsumer(consumer, closed);
        return endOfData();
      }

    };

    return AutoCloseableIterators.fromIterator(iterator, () -> closeConsumer(consumer, closed), null);
  }

  /**
   * Builds a per-stream (STREAM) state message for a single topic. The stream descriptor name
   * matches the catalog stream name (= topic name). STREAM is the state type modern Airbyte
   * platforms persist reliably — the previously used LEGACY type is deprecated and was silently
   * dropped by the platform, freezing the connection state.
   */
  private AirbyteMessage stateMessage(final String topic) {
    return new AirbyteMessage()
        .withType(AirbyteMessage.Type.STATE)
        .withState(new AirbyteStateMessage()
            .withType(AirbyteStateMessage.AirbyteStateType.STREAM)
            .withStream(new AirbyteStreamState()
                .withStreamDescriptor(new StreamDescriptor().withName(topic))
                .withStreamState(offsetState.toJson(topic))));
  }

  private void closeConsumer(final KafkaConsumer<?, ?> consumer, final AtomicBoolean closed) {
    if (closed.compareAndSet(false, true)) {
      LOGGER.debug("Closing Kafka consumer.");
      consumer.close();
    }
  }

  private Map<String, String> partitionProgress(final ConsumerRecords<?, ?> records) {
    final Map<String, String> progress = new TreeMap<>();
    records.partitions().forEach(partition -> {
      final List<? extends ConsumerRecord<?, ?>> partitionRecords = records.records(partition);
      final ConsumerRecord<?, ?> firstRecord = partitionRecords.get(0);
      final ConsumerRecord<?, ?> lastRecord = partitionRecords.get(partitionRecords.size() - 1);
      progress.put(partition.toString(),
          String.format("count=%d, offsets=%d..%d", partitionRecords.size(), firstRecord.offset(), lastRecord.offset()));
    });
    return progress;
  }

  private String getAutoOffsetReset() {
    return config.has("auto_offset_reset") ? config.get("auto_offset_reset").asText() : "latest";
  }

  protected Map<String, Object> getKafkaConfig() {

    final Map<String, Object> props = new HashMap<>();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.get("bootstrap_servers").asText());
    props.put(ConsumerConfig.GROUP_ID_CONFIG,
        config.has("group_id") ? config.get("group_id").asText() : null);
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
        config.has("max_poll_records") ? config.get("max_poll_records").intValue() : null);
    props.putAll(propertiesByProtocol(config));
    props.put(ConsumerConfig.CLIENT_ID_CONFIG,
        config.has("client_id") ? config.get("client_id").asText() : null);
    props.put(ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG, config.get("client_dns_lookup").asText());
    if (config.has("enable_auto_commit") && config.get("enable_auto_commit").booleanValue()) {
      LOGGER.warn("Ignoring enable_auto_commit=true. Kafka source offsets are managed through Airbyte state.");
    }
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
    props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,
        config.has("auto_commit_interval_ms") ? config.get("auto_commit_interval_ms").intValue() : null);
    props.put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG,
        config.has("retry_backoff_ms") ? config.get("retry_backoff_ms").intValue() : null);
    props.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,
        config.has("request_timeout_ms") ? config.get("request_timeout_ms").intValue() : null);
    props.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG,
        config.has("receive_buffer_bytes") ? config.get("receive_buffer_bytes").intValue() : null);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
        config.has("auto_offset_reset") ? config.get("auto_offset_reset").asText() : null);

    final Map<String, Object> filteredProps = props.entrySet().stream()
        .filter(entry -> entry.getValue() != null && !entry.getValue().toString().isBlank())
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

    return filteredProps;

  }

  private Map<String, Object> propertiesByProtocol(final JsonNode config) {
    final JsonNode protocolConfig = config.get("protocol");
    LOGGER.info("Kafka protocol config: {}", protocolConfig.toString());
    final KafkaProtocol protocol = KafkaProtocol.valueOf(protocolConfig.get("security_protocol").asText().toUpperCase());
    final ImmutableMap.Builder<String, Object> builder = ImmutableMap.<String, Object>builder()
        .put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, protocol.toString());

    switch (protocol) {
      case PLAINTEXT -> {}
      case SASL_SSL, SASL_PLAINTEXT -> {
        builder.put(SaslConfigs.SASL_JAAS_CONFIG, protocolConfig.get("sasl_jaas_config").asText());
        String saslMechanism = protocolConfig.get("sasl_mechanism").asText();
        builder.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
        if (saslMechanism.equals(OAuthBearerLoginModule.OAUTHBEARER_MECHANISM)) {
          builder.put(SaslConfigs.SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL, protocolConfig.get("oauthbearer_token_endpoint_url").asText());
          builder.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, OAuthBearerLoginCallbackHandler.class.getName());
        } else if (saslMechanism.equals("AWS_MSK_IAM")) {
          // IAMClientCallbackHandler
          builder.put(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS, IAMClientCallbackHandler.class.getName());
        }
      }
      default -> throw new RuntimeException("Unexpected Kafka protocol: " + Jsons.serialize(protocol));
    }

    return builder.build();
  }

}
