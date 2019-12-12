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

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.radarcns.connect.util.NotEmptyString;
import org.radarcns.connect.util.Utility;
//import org.radarcns.connect.util.ValidClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.common.config.ConfigDef.Importance.HIGH;
import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.NO_DEFAULT_VALUE;

/**
 * Configures the connection between Kafka and CSV File direct output.
 */
public class CsvFileSinkConnector extends SinkConnector {
    private static final Logger logger = LoggerFactory.getLogger(CsvFileSinkConnector.class);

    static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(TOPICS_CONFIG, Type.LIST, NO_DEFAULT_VALUE, HIGH,
                "List of topics to be streamed.");
    private Map<String, String> connectorConfig;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        List<String> errorMessages = new ArrayList<>();
        for (ConfigValue v : config().validate(props)) {
            if (!v.errorMessages().isEmpty()) {
                errorMessages.add("Property " + v.name() + " with value " + v.value()
                        + " does not validate: " + String.join("; ", v.errorMessages()));
            }
        }
        if (!errorMessages.isEmpty()) {
            throw new ConfigException("Configuration does not validate: \n\t"
                    + String.join("\n\t", errorMessages));
        }

        connectorConfig = new HashMap<>(props);
        logger.info(Utility.convertConfigToString(connectorConfig));
    }

    @Override
    public Class<? extends Task> taskClass() {
        return CsvFileSinkTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        logger.info("At most {} will be started", maxTasks);

        return Collections.nCopies(maxTasks, connectorConfig);
    }

    @Override
    public void stop() {
        logger.debug("Stop");
        // Nothing to do since it has no background monitoring.
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }
}
