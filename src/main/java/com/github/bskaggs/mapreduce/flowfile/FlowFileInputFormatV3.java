package com.github.bskaggs.mapreduce.flowfile;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class FlowFileInputFormatV3 extends FileInputFormat<Map<String, String>, InputStream> {
	@Override
	public RecordReader<Map<String, String>, InputStream> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		return new FlowFileRecordReaderV3();
	}
	
	@Override
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}
}
