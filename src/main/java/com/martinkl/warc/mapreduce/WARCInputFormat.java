package com.martinkl.warc.mapreduce;

import java.io.EOFException;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.martinkl.warc.WARCFileReader;
import com.martinkl.warc.WARCRecord;
import com.martinkl.warc.WARCWritable;


/**
 * Hadoop InputFormat for mapreduce jobs ('new' API) that want to process data in WARC files.
 *
 * Usage:
 *
 * ```java
 * Job job = new Job(getConf());
 * job.setInputFormatClass(WARCInputFormat.class);
 * ```
 *
 * Mappers should use a key of {@link org.apache.hadoop.io.LongWritable} (which is
 * 1 for the first record in a file, 2 for the second record, etc.) and a value of
 * {@link WARCWritable}.
 */
public class WARCInputFormat extends FileInputFormat<LongWritable, WARCWritable> {

    /**
     * Opens a WARC file (possibly compressed) for reading, and returns a RecordReader for accessing it.
     */
    @Override
    public RecordReader<LongWritable, WARCWritable> createRecordReader(InputSplit split,
                                                                       TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new WARCReader();
    }

    /**
     * Always returns false, as WARC files cannot be split.
     */
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    private static class WARCReader extends RecordReader<LongWritable, WARCWritable> {
        private final LongWritable key = new LongWritable();
        private final WARCWritable value = new WARCWritable();
        private WARCFileReader reader;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            reader = new WARCFileReader(context.getConfiguration(), ((FileSplit) split).getPath());
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            try {
                WARCRecord record = reader.read();
                key.set(reader.getRecordsRead());
                value.setRecord(record);
                return true;
            } catch (EOFException eof) {
                return false;
            }
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }

        @Override
        public float getProgress() throws IOException {
            return reader.getProgress();
        }

        @Override
        public LongWritable getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public WARCWritable getCurrentValue() throws IOException, InterruptedException {
            return value;
        }
    }
}
