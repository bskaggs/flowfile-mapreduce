package com.github.bskaggs.mapreduce.flowfile;

import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Test;

import com.github.bskaggs.mapreduce.flowfile.FlowFileV3BytesWritableInputFormat.FlowFileV3BytesWritableRecordReader;
import com.github.bskaggs.mapreduce.flowfile.FlowFileV3InputStreamInputFormat.FlowFileV3InputStreamRecordReader;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.util.ReflectionUtils;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class RecordReaderTest {

	public RecordReader getReader(Class inputFormatClass, String filename) throws IOException, InterruptedException {
		assertTrue(InputFormat.class.isAssignableFrom(inputFormatClass));
		Configuration conf = new Configuration(false);
		conf.set("fs.default.name", "file:///");

		File testFile = new File(filename);
		Path path = new Path(testFile.getAbsoluteFile().toURI());
		FileSplit split = new FileSplit(path, 0, testFile.length(), null);

		InputFormat inputFormat = ReflectionUtils.newInstance(inputFormatClass, conf);
		TaskAttemptContext context = new TaskAttemptContextImpl(conf, new TaskAttemptID());
		RecordReader reader = inputFormat.createRecordReader(split, context);
		reader.initialize(split, context);

		return reader;
	}

	
	@Test

	public void testOutput() throws IOException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration(false);
		conf.set("fs.default.name", "file:///");
	
		Job job = Job.getInstance(conf);
		job.setJarByClass(getClass());
		job.setInputFormatClass(FlowFileV3BytesWritableInputFormat.class);
		FlowFileV3BytesWritableInputFormat.setInputPaths(job, "src/test/resources");
		job.setNumReduceTasks(0);
		FileOutputFormat.setOutputPath(job, new Path("src/test/output"));
		job.setOutputFormatClass(FlowFileV3OutputFormat.class);
		job.waitForCompletion(true);
	}
	@Test
	public void testBytesWritable() throws IOException, InterruptedException {
		for (int j = 0; j < 100; j++) {
			FlowFileV3BytesWritableRecordReader reader = (FlowFileV3BytesWritableRecordReader) getReader(FlowFileV3BytesWritableInputFormat.class, "src/test/resources/nums.pkg");

			int count = 0;
			while (reader.nextKeyValue()) {
				count++;
				Map<String, String> attributes = reader.getCurrentKey();
				BytesWritable val = reader.getCurrentValue();

				String body = new String(val.getBytes(), 0, val.getLength(), StandardCharsets.UTF_8);
				String trimBody = body.trim();
				assertEquals(body, trimBody + "\n");
				String filename = attributes.get("filename");

				assertEquals(filename, "num_" + trimBody + ".txt");
			}
			assertEquals(100, count);
		}
	}

	@Test
	public void testInputStream() throws IOException, InterruptedException {

		for (int j = 0; j < 100; j++) {
			FlowFileV3InputStreamRecordReader reader = (FlowFileV3InputStreamRecordReader) getReader(FlowFileV3InputStreamInputFormat.class, "src/test/resources/nums.pkg");

			int count = 0;
			while (reader.nextKeyValue()) {
				count++;
				Map<String, String> attributes = reader.getCurrentKey();
				InputStream val = reader.getCurrentValue();

				String body = IOUtils.toString(val, StandardCharsets.UTF_8);
				String trimBody = body.trim();
				assertEquals(body, trimBody + "\n");
				String filename = attributes.get("filename");

				assertEquals(filename, "num_" + trimBody + ".txt");
			}
			assertEquals(100, count);
		}
	}
}
