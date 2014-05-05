package com.martinkl.warc;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WARCFileWriter {
    private static final Logger logger = LoggerFactory.getLogger(WARCFileWriter.class);
    public static final long DEFAULT_MAX_SEGMENT_SIZE = 1000000000L; // 1 GB

    private final Configuration conf;
    private final CompressionCodec codec;
    private final Path workOutputPath;
    private final Progressable progress;
    private final String extensionFormat;
    private final long maxSegmentSize;
    private long segmentsCreated = 0, segmentsAttempted = 0, bytesWritten = 0;
    private CountingOutputStream byteStream;
    private DataOutputStream dataStream;

    public WARCFileWriter(Configuration conf, CompressionCodec codec, Path workOutputPath) throws IOException {
        this(conf, codec, workOutputPath, null);
    }

    public WARCFileWriter(Configuration conf, CompressionCodec codec, Path workOutputPath, Progressable progress)
            throws IOException {
        this.conf = conf;
        this.codec = codec;
        this.workOutputPath = workOutputPath;
        this.progress = progress;
        this.extensionFormat = ".seg-%05d.attempt-%05d.warc" +
                (codec == null ? "" : codec.getDefaultExtension());
        this.maxSegmentSize = conf.getLong("warc.output.segment.size", DEFAULT_MAX_SEGMENT_SIZE);
        createSegment();
    }

    public static CompressionCodec getGzipCodec(Configuration conf) {
        try {
            return (CompressionCodec) ReflectionUtils.newInstance(
                conf.getClassByName("org.apache.hadoop.io.compress.GzipCodec").asSubclass(CompressionCodec.class),
                conf);
        } catch (ClassNotFoundException e) {
            logger.warn("GzipCodec could not be instantiated", e);
            return null;
        }
    }

    /**
     * Creates an output segment file and sets up the output streams to point at it.
     * If the file already exists, retries with a different filename. This is a bit nasty --
     * after all, {@link FileOutputFormat}'s work directory concept is supposed to prevent
     * filename clashes -- but it looks like Amazon Elastic MapReduce prevents use of per-task
     * work directories if the output of a job is on S3.
     *
     * <p>TODO: Investigate this and find a better solution.
     */
    private void createSegment() throws IOException {
        segmentsAttempted = 0;
        bytesWritten = 0;
        boolean success = false;

        while (!success) {
            Path path = workOutputPath.suffix(String.format(extensionFormat, segmentsCreated, segmentsAttempted));
            FileSystem fs = path.getFileSystem(conf);

            try {
                // The o.a.h.mapred OutputFormats overwrite existing files, whereas
                // the o.a.h.mapreduce OutputFormats don't overwrite. Bizarre...
                // Here, overwrite if progress != null, i.e. if using mapred API.
                FSDataOutputStream fsStream = (progress == null) ? fs.create(path, false): fs.create(path, progress);
                byteStream = new CountingOutputStream(new BufferedOutputStream(fsStream));
                dataStream = new DataOutputStream(codec == null ? byteStream : codec.createOutputStream(byteStream));
                segmentsCreated++;
                logger.info("Writing to output file: {}", path);
                success = true;

            } catch (IOException e) {
                if (e.getMessage().startsWith("File already exists")) {
                    logger.warn("Tried to create file {} but it already exists; retrying.", path);
                    segmentsAttempted++; // retry
                } else {
                    throw e;
                }
            }
        }
    }

    public void write(WARCRecord record) throws IOException {
        if (bytesWritten > maxSegmentSize) {
            dataStream.close();
            createSegment();
        }
        record.write(dataStream);
    }

    public void write(WARCWritable record) throws IOException {
        if (record.getRecord() != null) write(record.getRecord());
    }

    public void close() throws IOException {
        dataStream.close();
    }


    private class CountingOutputStream extends FilterOutputStream {
        public CountingOutputStream(OutputStream out) {
            super(out);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
            bytesWritten += len;
        }

        @Override
        public void write(int b) throws IOException {
            out.write(b);
            bytesWritten++;
        }

        // Overriding close() because FilterOutputStream's close() method pre-JDK8 has bad behavior:
        // it silently ignores any exception thrown by flush(). Instead, just close the delegate stream.
        // It should flush itself if necessary. (Thanks to the Guava project for noticing this.)
        @Override
        public void close() throws IOException {
            out.close();
        }
    }
}
