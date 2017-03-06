package com.github.bskaggs.mapreduce.flowfile;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.commons.io.input.BoundedInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class FlowFileV3InputStreamInputFormat extends FileInputFormat<Map<String, String>, InputStream> {
	@Override
	public RecordReader<Map<String, String>, InputStream> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		return new FlowFileV3InputStreamRecordReader();
	}
	
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}
	
	public static class FlowFileV3InputStreamRecordReader extends AbstractFlowFileV3RecordReader<InputStream> {

		@Override
		protected InputStream nextValue(FSDataInputStream fileStream, long byteLength) {
			BoundedInputStream value = new BoundedInputStream(fileStream, byteLength);
			value.setPropagateClose(false);
			return value;
		}

	}
}
