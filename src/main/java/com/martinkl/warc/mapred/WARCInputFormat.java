package com.martinkl.warc.mapred;

import java.io.EOFException;
import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import com.martinkl.warc.WARCFileReader;
import com.martinkl.warc.WARCRecord;
import com.martinkl.warc.WARCWritable;

public class WARCInputFormat extends FileInputFormat<LongWritable, WARCWritable> {

    @Override
    public RecordReader<LongWritable, WARCWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter)
            throws IOException {
        reporter.setStatus(split.toString());
        return new WARCReader(job, (FileSplit) split);
    }

    @Override
    protected boolean isSplitable(FileSystem fs, Path filename) {
        return false;
    }

    private static class WARCReader implements RecordReader<LongWritable, WARCWritable> {
        private final WARCFileReader reader;

        public WARCReader(JobConf job, FileSplit split) throws IOException {
            reader = new WARCFileReader(job, ((FileSplit) split).getPath());
        }

        @Override
        public LongWritable createKey() {
            return new LongWritable();
        }

        @Override
        public WARCWritable createValue() {
            return new WARCWritable();
        }

        @Override
        public boolean next(LongWritable key, WARCWritable value) throws IOException {
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
        public long getPos() throws IOException {
            return reader.getBytesRead();
        }

        @Override
        public float getProgress() throws IOException {
            return reader.getProgress();
        }

        @Override
        public void close() throws IOException {
            reader.close();
        }
    }
}
