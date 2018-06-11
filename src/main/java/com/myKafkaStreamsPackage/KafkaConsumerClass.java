package com.myKafkaStreamsPackage;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerClass {
    private final static String TOPIC = "streams-wordcount-output";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092, localhost:9093, localhost:9094";

    protected static Consumer<Long, String> createConsumer() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create the consumer using props.
        final Consumer<Long, String> consumer = new KafkaConsumer<>(props);

        // consumer.subscribe(Collections.singletonList(TOPIC));. The subscribe method takes a list of
        // topics to subscribe to, and this list will replace the current subscriptions if any.
        ((KafkaConsumer) consumer).subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }

    static void runConsumer() throws InterruptedException {
        final Consumer<Long, String> consumer = createConsumer();
        final int giveUp = 100;
        int noRecordsCount = 0;

        while(true) {
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
            if(consumerRecords.count() == 0) {
                noRecordsCount++;
                if(noRecordsCount > giveUp)
                    break;
                continue;
            }

            consumerRecords.forEach(longStringConsumerRecord -> {
                System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
                        longStringConsumerRecord.key(), longStringConsumerRecord.value(),
                        longStringConsumerRecord.partition(), longStringConsumerRecord.offset());

                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });

            consumer.commitAsync();
        }
        consumer.close();
        System.out.println("DONE");
    }

    public static void main(String... args) throws Exception {
        for(int i = 0; i < 1; i++)
            runConsumer();
    }
}
