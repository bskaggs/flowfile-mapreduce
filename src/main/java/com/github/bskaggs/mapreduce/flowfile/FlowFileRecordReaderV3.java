package com.github.bskaggs.mapreduce.flowfile;

/*
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
 */
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.io.input.BoundedInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FlowFileRecordReaderV3 extends RecordReader<Map<String, String>, InputStream> {
    public static final byte[] MAGIC_HEADER = {'N', 'i', 'F', 'i', 'F', 'F', '3'};
    private final byte[] headerBuffer = new byte[MAGIC_HEADER.length];
    
	private FSDataInputStream fileStream;
	private Map<String, String> key;
	private BoundedInputStream value;

	private long startPos;
	private long nextPos;
	private long lastPos;

	private long length;

    protected Map<String, String> readAttributes(Map<String, String> attributes, final FSDataInputStream in) throws IOException {
        final Integer numAttributes = readFieldLength(in); //read number of attributes
        if (numAttributes == 0) {
            throw new IOException("flow files cannot have zero attributes");
        }
        for (int i = 0; i < numAttributes; i++) { //read each attribute key/value pair
            final String key = readString(in);
            final String value = readString(in);
            attributes.put(key, value);
        }
        return attributes;
    }

    protected String readString(final FSDataInputStream in) throws IOException {
    	final int numBytes = readFieldLength(in);
        final byte[] bytes = new byte[numBytes];
        in.readFully(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private int readFieldLength(final FSDataInputStream in) throws IOException {
    	int value = in.readUnsignedShort();
    	if (value == 0xFFFF) {
        	value = in.readInt();
        }
        return value;
    }
        
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (nextPos >= lastPos) {
			return false;
		}
		
		if (fileStream.getPos() != nextPos) {
			fileStream.seek(nextPos);
		}
		
		fileStream.readFully(headerBuffer);
	    if (!Arrays.equals(headerBuffer, MAGIC_HEADER)) {
            throw new IOException("Not in FlowFile-v3 format");
        }
        
        key.clear();
        readAttributes(key, fileStream);
        
        long byteLength = fileStream.readLong();
        nextPos = fileStream.getPos() + byteLength;
        value = new BoundedInputStream(fileStream, byteLength);
        value.setPropagateClose(false);

		return true;
	}
	
	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		FileSplit fileSplit = (FileSplit) split;
  
	    Path file = fileSplit.getPath();
	    FileSystem fs = file.getFileSystem(context.getConfiguration());
	    fileStream = fs.open(file);
	    
	    startPos = fileSplit.getStart();
	    nextPos = startPos;
	    length = fileSplit.getLength();
	    lastPos = nextPos + length;
	}

	@Override
	public Map<String, String> getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public InputStream getCurrentValue() throws IOException, InterruptedException {
		return value;
	}
	
	@Override
	public float getProgress() throws IOException {
		return ((float) (fileStream.getPos() - startPos)) / length;
	}
	
	@Override
	public void close() throws IOException {
		fileStream.close();
	}
}
