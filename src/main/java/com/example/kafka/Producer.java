package com.example.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Properties;


public class Producer {
    public static void main(String[] args) {

        final Logger logger= LoggerFactory.getLogger(Producer.class);
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); //server
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        final KafkaProducer<String, String> producer = new KafkaProducer<String, String>(prop);



        for(int i=0; i<10; ++i)
        {
            ProducerRecord<String, String> record= new ProducerRecord<>("sample-topic","key_"+i, "value_"+i); //insert key value pairs

            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e==null)
                    {
                        logger.info("\n Received record \n" //log values
                                + "Topic:"+recordMetadata.topic()+", Partition:"
                                + recordMetadata.partition()+", "+
                                "Offset: "+ recordMetadata.offset()+
                                "  @timestamp:"+ recordMetadata.timestamp()+ "\n"
                        );
                    }
                    else
                    {
                        logger.error("Error occured", e); //error handler
                    }
                }
            });

        }


        producer.flush(); //writes unnwritten records to topic
        producer.close();
    }
}
