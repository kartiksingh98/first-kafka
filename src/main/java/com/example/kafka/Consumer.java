package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class Consumer  {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(Consumer.class); //for logging
        final String bootstrapServers = "localhost:9092"; //server
        final String consumerGroupID = "java-group-consumer";  //consumerid

        Properties p = new Properties();
        p.setProperty(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); //server
        p.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupID);
        p.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        final KafkaConsumer<String, String> consumer= new KafkaConsumer<String, String>(p);

        consumer.subscribe(Arrays.asList("sample-topic")); //topic id
        while(true)
        {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord record: records)
            {
                logger.info(
                        "\n Received record \n"
                                + "Topic:"+record.topic()+", Partition:"
                                + record.partition()+", "+
                                "Offset: "+ record.offset()+
                                "  @timestamp:"+ record.timestamp()+ "\n"

                );
            }

        }


    }

}
