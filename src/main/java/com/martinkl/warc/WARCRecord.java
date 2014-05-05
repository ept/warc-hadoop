package com.martinkl.warc;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class WARCRecord {

    public static final String WARC_VERSION = "WARC/1.0";
    private static final int MAX_LINE_LENGTH = 10000;
    private static final Pattern VERSION_PATTERN = Pattern.compile("WARC/[0-9\\.]+");
    private static final Pattern CONTINUATION_PATTERN = Pattern.compile("^[\\t ]+.*");
    private static final String CRLF = "\r\n";
    private static final byte[] CRLF_BYTES = { 13, 10 };

    private final WARCHeader header;
    private final byte[] content;

    public WARCRecord(DataInput in) throws IOException {
        header = readHeader(in);
        content = new byte[header.getContentLength()];
        in.readFully(content);
        readSeparator(in);
    }

    private static WARCHeader readHeader(DataInput in) throws IOException {
        String versionLine = readLine(in);
        if (!VERSION_PATTERN.matcher(versionLine).matches()) {
            throw new IllegalStateException("Expected WARC version, but got: " + versionLine);
        }

        LinkedHashMap<String, String> headers = new LinkedHashMap<String, String>();
        String line, fieldName = null;

        do {
            line = readLine(in);
            if (fieldName != null && CONTINUATION_PATTERN.matcher(line).matches()) {
                headers.put(fieldName, headers.get(fieldName) + line);
            } else if (!line.isEmpty()) {
                String[] field = line.split(":", 2);
                if (field.length < 2) throw new IllegalStateException("Malformed header line: " + line);
                fieldName = field[0].trim();
                headers.put(fieldName, field[1].trim());
            }
        } while (!line.isEmpty());

        return new WARCHeader(headers);
    }

    private static String readLine(DataInput in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        boolean seenCR = false, seenCRLF = false;
        while (!seenCRLF) {
            if (out.size() > MAX_LINE_LENGTH) {
                throw new IllegalStateException("Exceeded maximum line length");
            }
            byte b = in.readByte();
            if (!seenCR && b == 13) {
                seenCR = true;
            } else if (seenCR && b == 10) {
                seenCRLF = true;
            } else {
                seenCR = false;
                out.write(b);
            }
        }
        return out.toString("UTF-8");
    }

    private static void readSeparator(DataInput in) throws IOException {
        byte[] sep = new byte[4];
        in.readFully(sep);
        if (sep[0] != 13 || sep[1] != 10 || sep[2] != 13 || sep[3] != 10) {
            throw new IllegalStateException(String.format(
                "Expected final separator CR LF CR LF, but got: %d %d %d %d",
                sep[0], sep[1], sep[2], sep[3]));
        }
    }

    public WARCHeader getHeader() {
        return header;
    }

    public byte[] getContent() {
        return content;
    }

    public void write(DataOutput out) throws IOException {
        header.write(out);
        out.write(CRLF_BYTES);
        out.write(content);
        out.write(CRLF_BYTES);
        out.write(CRLF_BYTES);
    }

    @Override
    public String toString() {
        return header.toString();
    }


    public static class WARCHeader {
        private final Map<String, String> fields;

        public WARCHeader(Map<String, String> fields) {
            this.fields = fields;
        }

        public String getRecordType() {
            return fields.get("WARC-Type");
        }

        public String getDateString() {
            return fields.get("WARC-Date");
        }

        public String getRecordID() {
            return fields.get("WARC-Record-ID");
        }

        public String getContentType() {
            return fields.get("Content-Type");
        }

        public String getTargetURI() {
            return fields.get("WARC-Target-URI");
        }

        public int getContentLength() {
            String lengthStr = fields.get("Content-Length");
            if (lengthStr == null) throw new IllegalStateException("Missing Content-Length header");
            try {
                return Integer.parseInt(lengthStr);
            } catch (NumberFormatException e) {
                throw new IllegalStateException("Malformed Content-Length header: " + lengthStr);
            }
        }

        public String getField(String field) {
            return fields.get(field);
        }

        public void write(DataOutput out) throws IOException {
            out.write(toString().getBytes("UTF-8"));
        }

        @Override
        public String toString() {
            StringBuffer buf = new StringBuffer();
            buf.append(WARC_VERSION);
            buf.append(CRLF);
            for (Map.Entry<String, String> field : fields.entrySet()) {
                buf.append(field.getKey());
                buf.append(": ");
                buf.append(field.getValue());
                buf.append(CRLF);
            }
            return buf.toString();
        }
    }
}
