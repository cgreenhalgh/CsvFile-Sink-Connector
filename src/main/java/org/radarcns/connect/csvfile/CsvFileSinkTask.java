/*
 * Copyright 2017-2019 The University of Nottingham,
 * The Hyve and King's College London
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.radarcns.connect.csvfile;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Task to handle data coming from Kafka and send it to CSV files.
 */
public class CsvFileSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(CsvFileSinkTask.class);
    public static final String PROJECT_ID = "projectId";
    public static final String USER_ID = "userId";
    public static final String SOURCE_ID = "sourceId";
    public static final String TIME = "time";
    public static final String TIME_RECEIVED = "timeReceived";

    public CsvFileSinkTask() {
    }

    @Override
    public String version() {
        return new CsvFileSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
    }

    @Override
    public void put(Collection<SinkRecord> sinkRecords) {
        for (SinkRecord record : sinkRecords) {
            TopicPartition partition = new TopicPartition(record.topic(),
                    record.kafkaPartition());
	    if (! ( record.key() instanceof Struct ) ) {
		log.warn("Key is not struct at {}.{}: {}: {}",
				    partition, record.kafkaOffset(), record.key().getClass().getName(), record.key());
		continue;
	    }
	    Struct key = (Struct)record.key();
	    if (! ( record.value() instanceof Struct ) ) {
		log.warn("Value is not struct at {}.{}: {}: {}",
				  partition, record.kafkaOffset(), record.value().getClass().getName(), record.value());
		continue;
	    }
 	    Struct value = (Struct)record.value();
	    try {
 		String projectId = key.getString(PROJECT_ID);
		String userId = key.getString(USER_ID);
		String sourceId = key.getString(SOURCE_ID);
		log.debug("time is a {}", value.schema().field(TIME));
		Double time = value.getFloat64(TIME);
		// or timeCompleted (active)
		Double timeReceived = value.schema().field(TIME_RECEIVED)!=null ? value.getFloat64(TIME_RECEIVED) : null;
		log.info("{}.{}: project {} user {} source {} at time {} received {} : {}",
				partition, record.kafkaOffset(),
				projectId, userId, sourceId,
				time, timeReceived, value);
		// TODO
	    } catch (Exception e) {
		log.error("processing record {}.{}: {} -> {}: {}",
				partition, record.kafkaOffset(), 
				record.key(), record.value(), e);
		log.error("Caused by", e);
	    }
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
	// TODO ...
    }

    @Override
    public void stop() {
        //clean initialized resources
        log.info("Stopped CsvFileSinkTask");
    }
}
