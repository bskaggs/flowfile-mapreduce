package com.github.bskaggs.mapreduce.flowfile;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class FlowFileV3BytesWritableInputFormat extends FileInputFormat<Map<String, String>, BytesWritable> {
	@Override
	public RecordReader<Map<String, String>, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		return new FlowFileV3BytesWritableRecordReader();
	}
	
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}
	
	public class FlowFileV3BytesWritableRecordReader extends AbstractFlowFileV3RecordReader<BytesWritable> {
		@Override
		protected BytesWritable nextValue(FSDataInputStream fileStream, long byteLength) throws IOException {
			if (byteLength > Integer.MAX_VALUE) {
				throw new IllegalArgumentException("Byte length requested too big for byte array");
			}
			byte[] bytes = new byte[(int) byteLength];
			fileStream.readFully(bytes);
			value = new BytesWritable(bytes);
			return value;
		}
	
	}

}
