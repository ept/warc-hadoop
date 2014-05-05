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

public class WARCFileReader {
    private static final Logger logger = LoggerFactory.getLogger(WARCFileReader.class);

    private final long fileSize;
    private CountingInputStream byteStream = null;
    private DataInputStream dataStream = null;
    private long bytesRead = 0, recordsRead = 0;

    public WARCFileReader(Configuration conf, Path filePath) throws IOException {
        FileSystem fs = filePath.getFileSystem(conf);
        this.fileSize = fs.getFileStatus(filePath).getLen();
        logger.info("Reading from " + filePath);

        CompressionCodec codec = filePath.getName().endsWith(".gz") ?
                                 WARCFileWriter.getGzipCodec(conf) : null;
        byteStream = new CountingInputStream(new BufferedInputStream(fs.open(filePath)));
        dataStream = new DataInputStream(codec == null ? byteStream : codec.createInputStream(byteStream));
    }

    public WARCRecord read() throws IOException {
        WARCRecord record = new WARCRecord(dataStream);
        recordsRead++;
        return record;
    }

    public void close() throws IOException {
        if (dataStream != null) dataStream.close();
        byteStream = null;
        dataStream = null;
    }

    public long getRecordsRead() {
        return recordsRead;
    }

    public long getBytesRead() {
        return bytesRead;
    }

    public float getProgress() {
        if (fileSize == 0) return 1.0f;
        return (float) bytesRead / (float) fileSize;
    }

    public final class CountingInputStream extends FilterInputStream {
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
