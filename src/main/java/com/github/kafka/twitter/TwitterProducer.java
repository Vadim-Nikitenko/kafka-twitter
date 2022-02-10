package com.github.kafka.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {

    private Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
    private Properties prop;
    private Producer<String, String> producer;

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public void run() {
        loadProps();
        // create a twitter client
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);
        Client client = createTwitterClient(msgQueue);
        client.connect();

        // create a kafka producer
        producer = createKafkaProducer();

        // shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping the client");
            client.stop();
            logger.info("Closing the producer");
            producer.close();
            logger.info("Applications stopped");
        }));

        // loop to send tweets to kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                send(new ProducerRecord<>(prop.getProperty("topic.name"), null, msg), (recordMetadata, e) -> {
                    if (e != null) {
                        logger.error("Error producing a record", e);
                    }
                });
            }
        }
    }

    public void send(ProducerRecord<String, String> producerRecord, Callback callback) {
        producer.send(producerRecord, callback);
    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();

        List<String> terms = Lists.newArrayList("kafka");
        hosebirdEndpoint.trackTerms(terms);

        Authentication hosebirdAuth = new OAuth1(
                prop.getProperty("twitter.consumer.key"),
                prop.getProperty("twitter.consumer.secret"),
                prop.getProperty("twitter.token"),
                prop.getProperty("twitter.token.secret"));

        ClientBuilder builder = new ClientBuilder()
                .name("twitter-client")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();
    }

    private Producer<String, String> createKafkaProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, prop.getProperty("bootstrap.server"));
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, String.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, String.class.getName());

        // idempotence producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, String.valueOf(true));
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // compression configs
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");

        producer = new KafkaProducer<>(properties);

        return producer;
    }

    private void loadProps() {
        prop = new Properties();
        try {
            prop.load(TwitterProducer.class.getClassLoader().getResourceAsStream("twitter_auth.properties"));
            prop.load(TwitterProducer.class.getClassLoader().getResourceAsStream("kafka_producer.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public Producer<String, String> getProducer() {
        return producer;
    }

    public void setProducer(Producer<String, String> producer) {
        this.producer = producer;
    }
}
