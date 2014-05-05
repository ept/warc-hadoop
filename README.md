WARC Input and Output Formats for Hadoop
========================================

This is a simple library for working with [WARC (Web Archive)](http://en.wikipedia.org/wiki/Web_ARChive)
files in Hadoop. It provides InputFormats for reading and OutputFormats for writing WARC files in
MapReduce jobs (supporting both the 'old' `org.apache.hadoop.mapred` and the 'new'
`org.apache.hadoop.mapreduce` API).

WARC files are used to record the activity of a web crawler. They include both the HTTP requests
that were sent to servers, and the HTTP response received (including headers). WARC is an
[ISO standard](http://bibnum.bnf.fr/warc/WARC_ISO_28500_version1_latestdraft.pdf), and is used
(amongst others) by the [Internet Archive](https://archive.org/details/ExampleArcAndWarcFiles)
and [CommonCrawl](http://commoncrawl.org/navigating-the-warc-file-format/).

This warc-hadoop library was written in order to explore the [CommonCrawl](http://commoncrawl.org/)
data, a publicly available dump of billions of web pages. The data is made available for free as
a [public dataset](https://aws.amazon.com/datasets/41740) on AWS. If you want to process it, you
just need to pay for the computing capacity of processing it on AWS, or for the network bandwidth to
download it.

Using warc-hadoop
-----------------

At the moment you have to build it from source, but hopefully I can get it published to a Maven
repository soon. For now:

    $ git clone https://github.com/ept/warc-hadoop.git
    $ cd warc-hadoop
    $ ./gradlew install

Then add the following Maven dependency to your project:

```xml
<dependency>
    <groupId>com.martinkl.warc</groupId>
    <artifactId>warc-hadoop</artifactId>
    <version>0.1.0</version>
</dependency>
```

Now you can import either `com.martinkl.warc.mapred.WARCInputFormat` or
`com.martinkl.warc.mapreduce.WARCInputFormat` into your Hadoop job, depending on which version of
the API you are using. Example usage:

```java
JobConf job = new JobConf(conf, CommonCrawlTest.class);

FileInputFormat.addInputPath(job, new Path("/path/to/my/input"));
FileOutputFormat.setOutputPath(job, new Path("/path/for/my/output"));
FileOutputFormat.setCompressOutput(job, true);

job.setInputFormat(WARCInputFormat.class);
job.setOutputFormat(WARCOutputFormat.class);
job.setOutputKeyClass(NullWritable.class);
job.setOutputValueClass(WARCWritable.class);
```

Example of a mapper that emits server responses, using the URL as the key:

```java
public static class MyMapper extends MapReduceBase
        implements Mapper<LongWritable, WARCWritable, Text, WARCWritable> {

    public void map(LongWritable key, WARCWritable value, OutputCollector<Text, WARCWritable> collector,
                    Reporter reporter) throws IOException {
        String recordType = value.getRecord().getHeader().getRecordType();
        String targetURL  = value.getRecord().getHeader().getTargetURI();

        if (recordType.equals("response") && targetURL != null) {
            collector.collect(new Text(targetURL), value);
        }
    }
}
```

File format parsing
-------------------

A WARC file consists of a flat sequence of records. Each record may be a HTTP request
(`recordType = "request"`), a response (`recordType = "response"`) or one of various other types,
including metadata. When reading from a WARC file, the records are given to the mapper one at a
time. That means that the request and the response will appear in two separate calls of the `map`
method.

This library currently doesn't perform any parsing of the data inside records, such as the HTTP
headers or the HTML body. You can simply read the server's response as an array of bytes.
Additional parsing functionality may be added in future versions.

WARC files are typically gzip-compressed. Gzip files are not splittable by Hadoop (i.e. an entire
file must be processed sequentially, it's not possible to start reading in the middle of a file) so
projects like CommonCrawl typically aim for a maximum file size of 1GB (compressed). If you're only
doing basic parsing, a file of that size takes less than a minute to process.

When writing WARC files, this library automatically splits output files into gzipped segments of
approximately 1GB. You can customize the segment size using the configuration key
`warc.output.segment.size` (the value is the target segment size in bytes).

Meta
----

(c) 2014 Martin Kleppmann. MIT License.

Please submit pull requests to the [Github project](https://github.com/ept/warc-hadoop).
