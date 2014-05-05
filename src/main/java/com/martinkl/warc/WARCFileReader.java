package com.martinkl.warc;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads {@link WARCRecord}s from a WARC file, using Hadoop's filesystem APIs. (This means you
 * can read from HDFS, S3 or any other filesystem supported by Hadoop). This implementation is
 * not tied to the MapReduce APIs -- that link is provided by the mapred
 * {@link com.martinkl.warc.mapred.WARCInputFormat} and the mapreduce
 * {@link com.martinkl.warc.mapreduce.WARCInputFormat}.
 */
public class WARCFileReader {
    private static final Logger logger = LoggerFactory.getLogger(WARCFileReader.class);

    private final long fileSize;
    private CountingInputStream byteStream = null;
    private DataInputStream dataStream = null;
    private long bytesRead = 0, recordsRead = 0;

    /**
     * Opens a file for reading. If the filename ends in `.gz`, it is automatically decompressed
     * on the fly.
     * @param conf The Hadoop configuration.
     * @param filePath The Hadoop path to the file that should be read.
     * @throws IOException
     */
    public WARCFileReader(Configuration conf, Path filePath) throws IOException {
        FileSystem fs = filePath.getFileSystem(conf);
        this.fileSize = fs.getFileStatus(filePath).getLen();
        logger.info("Reading from " + filePath);

        CompressionCodec codec = filePath.getName().endsWith(".gz") ?
                                 WARCFileWriter.getGzipCodec(conf) : null;
        byteStream = new CountingInputStream(new BufferedInputStream(fs.open(filePath)));
        dataStream = new DataInputStream(codec == null ? byteStream : codec.createInputStream(byteStream));
    }

    /**
     * Reads the next record from the file.
     * @return The record that was read.
     * @throws IOException
     */
    public WARCRecord read() throws IOException {
        WARCRecord record = new WARCRecord(dataStream);
        recordsRead++;
        return record;
    }

    /**
     * Closes the file. No more reading is possible after the file has been closed.
     * @throws IOException
     */
    public void close() throws IOException {
        if (dataStream != null) dataStream.close();
        byteStream = null;
        dataStream = null;
    }

    /**
     * Returns the number of records that have been read since the file was opened.
     */
    public long getRecordsRead() {
        return recordsRead;
    }

    /**
     * Returns the number of bytes that have been read from file since it was opened.
     * If the file is compressed, this refers to the compressed file size.
     */
    public long getBytesRead() {
        return bytesRead;
    }

    /**
     * Returns the proportion of the file that has been read, as a number between 0.0
     * and 1.0.
     */
    public float getProgress() {
        if (fileSize == 0) return 1.0f;
        return (float) bytesRead / (float) fileSize;
    }

    private class CountingInputStream extends FilterInputStream {
        public CountingInputStream(InputStream in) {
            super(in);
        }

        @Override
        public int read() throws IOException {
            int result = in.read();
            if (result != -1) bytesRead++;
            return result;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            int result = in.read(b, off, len);
            if (result != -1) bytesRead += result;
            return result;
        }

        @Override
        public long skip(long n) throws IOException {
            long result = in.skip(n);
            bytesRead += result;
            return result;
        }
    }
}
