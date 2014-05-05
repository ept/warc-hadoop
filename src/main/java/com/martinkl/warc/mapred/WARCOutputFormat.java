package com.martinkl.warc.mapred;

import java.io.IOException;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import com.martinkl.warc.WARCFileWriter;
import com.martinkl.warc.WARCWritable;

/**
 * Hadoop OutputFormat for mapred jobs ('old' API) that want to write data to WARC files.
 *
 * Usage:
 *
 * ```java
 * JobConf job = new JobConf(getConf());
 * job.setOutputFormat(WARCOutputFormat.class);
 * job.setOutputKeyClass(NullWritable.class);
 * job.setOutputValueClass(WARCWritable.class);
 * FileOutputFormat.setCompressOutput(job, true);
 * ```
 *
 * The tasks generating the output (usually the reducers, but may be the mappers if there
 * are no reducers) should use `NullWritable.get()` as the output key, and the
 * {@link WARCWritable} as the output value.
 */
public class WARCOutputFormat extends FileOutputFormat<NullWritable, WARCWritable> {

    /**
     * Creates a new output file in WARC format, and returns a RecordWriter for writing to it.
     */
    @Override
    public RecordWriter<NullWritable, WARCWritable> getRecordWriter(FileSystem fs, JobConf job, String filename,
                                                                    Progressable progress) throws IOException {
        return new WARCWriter(job, filename, progress);
    }

    private static class WARCWriter implements RecordWriter<NullWritable, WARCWritable> {
        private final WARCFileWriter writer;

        public WARCWriter(JobConf job, String filename, Progressable progress) throws IOException {
            CompressionCodec codec = getCompressOutput(job) ? WARCFileWriter.getGzipCodec(job) : null;
            Path workFile = FileOutputFormat.getTaskOutputPath(job, filename);
            this.writer = new WARCFileWriter(job, codec, workFile, progress);
        }

        @Override
        public void write(NullWritable key, WARCWritable value) throws IOException {
            writer.write(value);
        }

        @Override
        public void close(Reporter reporter) throws IOException {
            writer.close();
        }
    }
}
