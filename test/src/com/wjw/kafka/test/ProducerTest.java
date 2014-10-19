/**
 * Created by wjw on 14-10-19.
 */
package com.wjw.kafka.test;

import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class ProducerTest {
    public static void main(String[] args) {
        //long events = Long.parseLong(args[0]);
        Random rnd = new Random();

        Properties props = new Properties();
        // props.put("zookeeper.connect", "127.0.0.1:2181");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list","10.128.12.72:9092");
//        props.put("metadata.broker.list", "broker1:9092,broker2:9092 ");
//        props.put("serializer.class", "kafka.serializer.StringEncoder");
//        props.put("partitioner.class", "example.producer.SimplePartitioner");
//        props.put("request.required.acks", "1");

        ProducerConfig config = new ProducerConfig(props);

        Producer<String, String> producer = new Producer<String, String>(config);

        for (long nEvents = 0; nEvents < 10; nEvents++) {
            long runtime = new Date().getTime();
            String ip = "192.168.2." + rnd.nextInt(255);
            String msg = runtime + ",www.example.com," + ip;
            KeyedMessage<String, String> data = new KeyedMessage<String, String>("page_visits", ip, msg);
            producer.send(data);
        }
        producer.close();
    }
}

