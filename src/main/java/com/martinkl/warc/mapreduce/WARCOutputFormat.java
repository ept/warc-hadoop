package com.martinkl.warc.mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.martinkl.warc.WARCFileWriter;
import com.martinkl.warc.WARCWritable;

public class WARCOutputFormat extends FileOutputFormat<NullWritable, WARCWritable> {

    @Override
    public RecordWriter<NullWritable, WARCWritable> getRecordWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new WARCWriter(context);
    }

    private class WARCWriter extends RecordWriter<NullWritable, WARCWritable> {
        private final WARCFileWriter writer;

        public WARCWriter(TaskAttemptContext context) throws IOException {
            Configuration conf = context.getConfiguration();
            CompressionCodec codec = getCompressOutput(context) ? WARCFileWriter.getGzipCodec(conf) : null;
            Path workFile = getDefaultWorkFile(context, "");
            this.writer = new WARCFileWriter(conf, codec, workFile);
        }

        @Override
        public void write(NullWritable key, WARCWritable value) throws IOException, InterruptedException {
            writer.write(value);
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            writer.close();
        }
    }
}
