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

import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.IllegalWorkerStateException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

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
    private File directory;
    class OutFile {
	File parent;
	File path;
	CSVWriter writer;
	List<String> headings;
        OutFile(File parent, File path, CSVWriter writer, List<String> headings) {
	    this.parent = parent;
	    this.path = path;
	    this.writer = writer;
	    this.headings = headings;
	}
    }
    // parent -> OutFile
    private Map<File,OutFile> outFiles = new HashMap<File,OutFile>();
    
    public CsvFileSinkTask() {
    }

    @Override
    public String version() {
        return new CsvFileSinkConnector().version();
    }

    @Override
    public void start(Map<String, String> props) {
	directory = new File(props.get(CsvFileSinkConnector.DIRECTORY_CONFIG));
	if (!directory.isDirectory()) {
	    if (directory.exists()) 
		throw new RuntimeException("Output directory "+directory+" exists but is not a directory");
	    else
		throw new RuntimeException("Output directory "+directory+" does not exist");
	}
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
		
		File parent = getParent(projectId, userId, record.topic());
		File path = getPath(parent, time);
 		OutFile of = null;
		synchronized (outFiles) {
		    of = outFiles.get(path);
		    if (of != null && of.path != path) {
		 	log.debug("close {}, open {}", of.path, path);
			closeOutFile(of);
			of = null;
			outFiles.remove(parent);
		    }
		    if (of == null) {
		    	of = openOutFile(parent, path, key, value);
		    	outFiles.put(parent, of);	
    		    }
		}
		synchronized (of) {
		    writeRecord(of, key, value);
		}
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
	for (OutFile of : outFiles.values()) {
	    try {
		of.writer.flush();
	    } catch (Exception e) {
		log.error("Flushing {}: {}", of.path, e);
	    }
	}
    }

    @Override
    public void stop() {
        //clean initialized resources
        for (OutFile of : outFiles.values()) {
            closeOutFile(of);
        }
        log.info("Stopped CsvFileSinkTask");
    }
    private String safeFilename(String s) {
	// space -> '-'; letter, number, underscore, - OK
	s = s.replace(" ", "-");
	s = s.replace("[^a-zA-Z0-9_-]","");
	return s;
    }
    private File getParent(String projectId, String userId, String topic) {
	File project = new File(directory, safeFilename(projectId));
	if (! project.exists() ) {
	    log.info("Create project directory {}", project);
	    project.mkdir();
	}
	File user = new File(project, safeFilename(userId));
	if (! user.exists() ) {
	    log.info("Create user directory {}", user);
	    user.mkdir();
	}
	File parent = new File(user, safeFilename(topic));
	if (! parent.exists() ) {
	    log.info("Create topic directory {}", parent);
	    parent.mkdir();
	}
	return parent;
    }
    private File getPath(File parent, double time) {
	SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd_HH");
	format.setTimeZone(TimeZone.getTimeZone("UTC"));
	// times are seconds
	String filename = format.format(new Date((long)(1000*time)))+".csv";
	return new File(parent, filename);
    }    
    private void closeOutFile(OutFile of) {
	try {
	    of.writer.close();
	} catch (Exception e) {
	    log.error("Closing {}: {}", of.path, e);
	}
    }
    private OutFile openOutFile(File parent, File path, Struct key, Struct value) throws FileNotFoundException {
        log.debug("Open file {}", path);
	boolean needsHeader = true; // default
	List<String> headers = null;
	if (path.exists()) {
	    // read header?!
	    try {
		CSVReader reader = new CSVReader(new BufferedReader(new InputStreamReader(new FileInputStream(path), StandardCharsets.UTF_8)));
		String [] csvheaders = reader.readNext();
		if ( csvheaders!= null && csvheaders.length >=5 ) {
		    log.debug("Read existing header from {}: {}", path, csvheaders);
		    headers = Arrays.asList(csvheaders);
		    needsHeader = false;
		} else {
		    log.warn("No heading found in existing file {}", path);
		}
		reader.close();
	    } catch (Exception e) {
		log.warn("Error checking header in {}: {}", path, e);
	    }
	}
	CSVWriter writer = null;
	if (needsHeader) {
	     // TODO	
	     writer = new CSVWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path), StandardCharsets.UTF_8)));
	     headers = writeHeader(writer, key, value);
	} else {
	    // append
	    writer = new CSVWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path, true))));
	}
	OutFile of = new OutFile(parent, path, writer, headers);
	return of;
    }
    private List<String> writeHeader(CSVWriter writer, Struct key, Struct value) {
	List<String> headers = new LinkedList<String>();
	addHeaders(headers, key, key.schema(), "key");
	addHeaders(headers, value, value.schema(), "value");
	writer.writeNext(headers.toArray(new String[headers.size()]));
	return headers;
    }
    private void addHeaders(List<String> headers, Object value, Schema schema, String prefix) {
 	if (value==null) {
	    headers.add(prefix);
	    return;
	}
	if (value instanceof Struct) {
	    Struct struct = (Struct)value;
	    for (Field field : struct.schema().fields()) {
		addHeaders(headers, struct.get(field), field.schema(), prefix+"."+field.name());
	    }
	} else if (value instanceof List<?>) {
	    List<Object> list = (List<Object>)value;
	    int ix = 0;
	    for (Object el : list) {
		addHeaders(headers, el, schema.valueSchema(), prefix+"."+ix);
		ix++;
	    }
	} else if (value instanceof Map) {
	    Map<?,?> map = (Map)value;
	    Map<String,Object> stringMap = new HashMap<String,Object>();
            for(Map.Entry<?,?> entry : map.entrySet()) {
		stringMap.put(entry.getKey().toString(), entry.getValue());
	    }
	    List<String> sortedKeys = new ArrayList<>(stringMap.keySet());
	    Collections.sort(sortedKeys);
	    for(String key : sortedKeys) {
	        addHeaders(headers, stringMap.get(key), schema.valueSchema(), prefix+"."+key);
	    }
	} else {
	    headers.add(prefix);
	}
    }
    private void writeRecord(OutFile of, Struct key, Struct value) {
	// TODO
    }
}
