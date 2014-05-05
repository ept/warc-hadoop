package com.martinkl.warc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;


public class WARCWritable implements Writable {

    private WARCRecord record;

    public WARCWritable() {
        this.record = null;
    }

    public WARCWritable(WARCRecord record) {
        this.record = record;
    }

    public WARCRecord getRecord() {
        return record;
    }

    public void setRecord(WARCRecord record) {
        this.record = record;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        if (record != null) record.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        record = new WARCRecord(in);
    }
}
