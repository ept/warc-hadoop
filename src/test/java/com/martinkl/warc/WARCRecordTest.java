package com.martinkl.warc;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import org.junit.Test;
import static org.junit.Assert.*;

public class WARCRecordTest {
    @Test
    public void testParseWARCInfo() throws IOException {
        StringBuffer buffer = new StringBuffer();
        buffer.append("WARC/1.0\r\n");
        buffer.append("WARC-Type: warcinfo\r\n");
        buffer.append("WARC-Date: 2014-03-18T17:47:38Z\r\n");
        buffer.append("WARC-Record-ID: <urn:uuid:d9bbb325-c09f-473c-8600-1c9dbd4ec443>\r\n");
        buffer.append("Content-Length: 371\r\n");
        buffer.append("Content-Type: application/warc-fields\r\n");
        buffer.append("WARC-Filename: CC-MAIN-20140313024455-00000-ip-10-183-142-35.ec2.internal.warc.gz\r\n");
        buffer.append("\r\n");
        buffer.append("robots: classic\r\n");
        buffer.append("hostname: ip-10-183-142-35.ec2.internal\r\n");
        buffer.append("software: Nutch 1.6 (CC)/CC WarcExport 1.0\r\n");
        buffer.append("isPartOf: CC-MAIN-2014-10\r\n");
        buffer.append("operator: CommonCrawl Admin\r\n");
        buffer.append("description: Wide crawl of the web with URLs provided by Blekko for March 2014\r\n");
        buffer.append("publisher: CommonCrawl\r\n");
        buffer.append("format: WARC File Format 1.0\r\n");
        buffer.append("conformsTo: http://bibnum.bnf.fr/WARC/WARC_ISO_28500_version1_latestdraft.pdf\r\n");
        buffer.append("\r\n");
        buffer.append("\r\n");
        buffer.append("\r\n");
        DataInputStream stream = new DataInputStream(new ByteArrayInputStream(buffer.toString().getBytes("UTF-8")));
        WARCRecord record = new WARCRecord(stream);
        assertEquals(371, record.getHeader().getContentLength());
        assertEquals("warcinfo", record.getHeader().getRecordType());
        assertEquals("2014-03-18T17:47:38Z", record.getHeader().getDateString());
        assertEquals("<urn:uuid:d9bbb325-c09f-473c-8600-1c9dbd4ec443>", record.getHeader().getRecordID());
        assertEquals("application/warc-fields", record.getHeader().getContentType());
        assertNull(record.getHeader().getTargetURI());
    }
}
