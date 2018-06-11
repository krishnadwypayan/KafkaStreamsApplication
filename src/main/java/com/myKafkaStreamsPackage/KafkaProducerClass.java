package com.myKafkaStreamsPackage;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class KafkaProducerClass {
    private final static String TOPIC = "streams-plaintext-input";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092, localhost:9093, localhost:9094";

    /**
     * To create a Kafka producer, you use java.util.Properties and
     * define certain properties that we pass to the constructor of a KafkaProducer.
     *
     * createProducer() sets the BOOTSTRAP_SERVERS_CONFIG (“bootstrap.servers) property
     * to the list of broker addresses.
     *
     * The CLIENT_ID_CONFIG (“client.id”) is an id to pass to the server when making requests
     * so the server can track the source of requests beyond just IP/port by passing a producer
     * name for things like server-side request logging.
     *
     * The KEY_SERIALIZER_CLASS_CONFIG (“key.serializer”) is a Kafka Serializer class for Kafka
     * record keys that implements the Kafka Serializer interface. Notice that we set this to
     * LongSerializer as the message ids in our example are longs.
     *
     * The VALUE_SERIALIZER_CLASS_CONFIG (“value.serializer”) is a Kafka Serializer class for Kafka
     * record values that implements the Kafka Serializer interface. Notice that we set this to
     * StringSerializer as the message body in our example are strings.
     * @return
     */

    private static Producer<Long, String> createProducer() {
        /**
         * To create a Kafka producer, you use java.util.Properties and
         * define certain properties that we pass to the constructor of a KafkaProducer.
         */
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }

    static void runProducer(final int sendMessageCount) throws Exception {
        /**
         * This method just iterates through a for loop, creating a ProducerRecord
         * sending an example message ("Hello Mom " + index) as the record value and
         * the for loop index as the record key. For each iteration, runProducer
         * calls the send method of the producer (RecordMetadata metadata = producer.send(record).get()).
         * The send method returns a Java Future.
         *
         * The response RecordMetadata has "partition" where the record was written
         * and the ‘offset’ of the record in that partition.
         *
         * Notice the call to flush and close. Kafka will auto flush on its own,
         * but you can also call flush explicitly which will send the accumulated records now.
         * It is polite to close the connection when we are done.
         */
        final Producer<Long, String> producer = createProducer();
        long time = System.currentTimeMillis();
        final CountDownLatch countDownLatch = new CountDownLatch(sendMessageCount);

        try {
            for(long index = time; index < time + sendMessageCount; index++) {
                final ProducerRecord<Long, String> record = new ProducerRecord<>(TOPIC, index, "Hello Mom " + index);


//                  RecordMetadata metadata = producer.send(record).get();
//                                long elapsedTime = System.currentTimeMillis() - time;
//                                System.out.printf("sent record(key=%s value=%s) " +
//                                                "meta(partition=%d, offset=%d) time=%d\n",
//                                        record.key(), record.value(), metadata.partition(),
//                                        metadata.offset(), elapsedTime);


                /**
                 * Kafka provides an asynchronous send method to send a record to a topic.
                 * Let’s use this method to send some message ids and messages to the
                 * Kafka topic we created earlier. The big difference here will be that
                 * we use a lambda expression to define a callback.
                 */

                producer.send(record, (metadata, exception) -> {
                    long elapsedTime = System.currentTimeMillis() - time;

                    if(metadata != null) {
                        System.out.printf("sent record(key=%s value=%s) " + "meta(partition=%d, offset=%d) time=%d\n",
                                record.key(), record.value(), metadata.partition(), metadata.offset(), elapsedTime);
                    } else {
                        exception.printStackTrace();
                    }
                    countDownLatch.countDown();
                });
                Thread.sleep(10);
            }
            countDownLatch.await(25, TimeUnit.SECONDS);
        } finally {
            producer.flush();
            producer.close();
        }
    }

    public static void main(String... args) throws Exception {
        if(args.length == 0) {
            for(int i = 0; i < 1; i++)
                runProducer(25);
        }
        else
            runProducer(Integer.parseInt(args[0]));
    }
}
