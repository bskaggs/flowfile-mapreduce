package com.github.bskaggs.mapreduce.flowfile;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FlowFileV3OutputFormat extends FileOutputFormat<Map<String, String>, BytesWritable> {

	@Override
	public RecordWriter<Map<String, String>, BytesWritable> getRecordWriter(TaskAttemptContext job) throws IOException, InterruptedException {
		Configuration conf = job.getConfiguration();
		Path file = getDefaultWorkFile(job, ".pkg");
		FileSystem fs = file.getFileSystem(conf);
		FSDataOutputStream fileOut = fs.create(file, false);
		return new FlowFileV3RecordWriter(fileOut);
	}
}
