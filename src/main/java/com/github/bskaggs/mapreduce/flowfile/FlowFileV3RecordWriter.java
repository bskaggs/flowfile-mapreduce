package com.github.bskaggs.mapreduce.flowfile;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class FlowFileV3RecordWriter extends RecordWriter<Map<String, String>, BytesWritable> {
    public static final byte[] MAGIC_HEADER = {'N', 'i', 'F', 'i', 'F', 'F', '3'};
    private static final int MAX_VALUE_2_BYTES = 0xFFFF;
	
	private final DataOutputStream out;
	
	public FlowFileV3RecordWriter(DataOutputStream out) {
		this.out = out;
	}

	@Override
	public void write(Map<String, String> attributes, BytesWritable value) throws IOException, InterruptedException {
        out.write(MAGIC_HEADER);
        if (attributes == null) {
            writeFieldLength(0);
        } else {
            writeFieldLength(attributes.size()); //write out the number of attributes
            for (final Map.Entry<String, String> entry : attributes.entrySet()) { //write out each attribute key/value pair
                writeString(entry.getKey());
                writeString(entry.getValue());
            }
        }

        out.writeLong(value.getLength()); //write out length of data
        out.write(value.getBytes(), 0, value.getLength());
 	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		out.close();
	}

    private void writeString(final String val) throws IOException {
    	final byte[] bytes = val.getBytes(StandardCharsets.UTF_8);
        writeFieldLength(bytes.length);
        out.write(bytes);
    }

    private void writeFieldLength(final int numBytes) throws IOException {
        if (numBytes < MAX_VALUE_2_BYTES) {
        	out.writeShort(numBytes);
        } else {
        	out.writeShort(MAX_VALUE_2_BYTES);
        	out.writeInt(numBytes);
        }
    }
}
