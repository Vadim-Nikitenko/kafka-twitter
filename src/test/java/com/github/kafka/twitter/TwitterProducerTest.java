package com.github.kafka.twitter;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;

public class TwitterProducerTest {
    MockProducer<String, String> mockProducer;
    TwitterProducer twitterProducer;

    private ByteArrayOutputStream systemOutContent;
    private ByteArrayOutputStream systemErrContent;
    private final PrintStream originalSystemOut = System.out;
    private final PrintStream originalSystemErr = System.err;

    @Before
    public void setUp() {
        mockProducer = new MockProducer<>(false, new StringSerializer(), new StringSerializer());
        twitterProducer = new TwitterProducer();
        twitterProducer.setProducer(mockProducer);
    }

    @Before
    public void setUpStreams() {
        systemOutContent = new ByteArrayOutputStream();
        systemErrContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(systemOutContent));
        System.setErr(new PrintStream(systemErrContent));
    }

    @After
    public void restoreStreams() {
        System.setOut(originalSystemOut);
        System.setErr(originalSystemErr);
    }


    @Test
    public void testPublishRecord_sent_data() {
        twitterProducer.send(new ProducerRecord<>("tweets_topic", null, "Test data"), null);
        mockProducer.completeNext();
        List<ProducerRecord<String, String>> records = mockProducer.history();
        Assert.assertEquals(1, records.size());
        ProducerRecord<String, String> record = records.get(0);
        Assert.assertEquals(null, record.key());
        Assert.assertEquals("Test data", record.value());
        Assert.assertEquals("tweets_topic", record.topic());
        System.out.print("key=" + record.key() + ", value=" + record.value());
        Assert.assertEquals("key=null, value=Test data", systemOutContent.toString());
    }

}
