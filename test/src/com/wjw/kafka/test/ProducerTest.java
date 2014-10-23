/**
 * Created by wjw on 14-10-19.
 */
package com.wjw.kafka.test;

import java.io.IOException;
import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import reader.MyFileReader;
import reader.MyReader;

public class ProducerTest {
    static ProducerConfig config = null;

    static Producer<String, String> producer = null;
    static Properties props = new Properties();
    public static void init() {
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("metadata.broker.list","10.128.12.72:9092");
        config = new ProducerConfig(props);
        producer = new Producer<String, String>(config);
    }
    public static void main(String[] args) {
        //long events = Long.parseLong(args[0]);


        // props.put("zookeeper.connect", "127.0.0.1:2181");

//        props.put("metadata.broker.list", "broker1:9092,broker2:9092 ");
//        props.put("serializer.class", "kafka.serializer.StringEncoder");
//        props.put("partitioner.class", "example.producer.SimplePartitioner");
//        props.put("request.required.acks", "1");


//        String[] sentences = {
//                "the cow jumped over the moon",
//                "an apple a day keeps the doctor away",
//                "four score and seven years ago",
//                "snow white and the seven dwarfs",
//                "i am at two with nature"
//        };
//        for (long nEvents = 0; nEvents < 10; nEvents++) {
//            long runtime = new Date().getTime();
//            String ip = "192.168.2." + rnd.nextInt(255);
//            String msg = runtime + ",www.example.com," + ip;
//            KeyedMessage<String, String> data = new KeyedMessage<String, String>("page_visits", ip, msg);
//            producer.send(data);
//        }
//        producer.close();

//        int length = sentences.length;
//        int i = 1000000;
//        while(i > 1) {
//            try {
//                Thread.sleep(10L);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            Random random = new Random();
//            String msg = sentences[random.nextInt(length)];
//            //long runtime = new Date().getTime();
//            String ip = "192.168.2." + rnd.nextInt(255);
//            KeyedMessage<String, String> data = new KeyedMessage<String, String>("page_visits", ip, msg);
//            producer.send(data);
//            i--;
//        }
//        producer.close();
        String filePath = "/home/wjw/tmp_test";
        init();
        readFiles(filePath);
        producer.close();
    }
    public static void readFiles(String path) {
        MyFileReader myFileReader = new MyFileReader();
        try {
            myFileReader.findFile(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
        Iterator<String> iterator = myFileReader.getFileList().iterator();
        while(iterator.hasNext()) {
            String file = iterator.next();
            Iterator<String> content = MyReader.readFileByLines(file).iterator();
             while(content.hasNext()) {
                 sendMsg(content.next());
                 //System.out.println(content.next());
             }
        }

    }
    public static void sendMsg(String msg) {
        //long runtime = new Date().getTime();
        Random rnd = new Random();
        String ip = "192.168.2." + rnd.nextInt(255);
        KeyedMessage<String, String> data = new KeyedMessage<String, String>("page_visits", ip, msg);
        producer.send(data);
    }
}

