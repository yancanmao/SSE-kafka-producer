package com.colobu.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.*;
import java.util.*;
import org.apache.log4j.Logger;

/**
 * Producer例子，可以往topic上发送指定数量的消息.
 * 消息格式为: 发送时间,编号,网址,ip
 */
public class ProducerExample {

	private String TOPIC;

	private static KafkaProducer<String, String> producer;

	public ProducerExample(String input) {
		TOPIC = input;
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("client.id", "ProducerExample");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<String, String>(props);

    }
    
    public void generate(String file) throws InterruptedException {
    //public void generate(String file) {

        String sCurrentLine;
        List<String> textList = new ArrayList<>();
        FileReader stream = null; 
        // // for loop to generate message
        BufferedReader br = null;
        int sent_sentences = 0;
        long cur = 0;
        long start = 0;
        long interval = 0;
        long inter = 0;
        try {
            //stream = new FileReader("/root/share/sortSBStream.txt");
			stream = new FileReader("/root/share/"+file+".txt");
            br = new BufferedReader(stream);
            Thread.sleep(10000);
            while ((sCurrentLine = br.readLine()) != null) {
                if (sCurrentLine.equals("end")) {
                    start = System.nanoTime();
                    interval = 1000000000/textList.size();
                    //if (textList.size() > 70000){ 
                    for (int i=0; i<textList.size(); i++) {
                        cur = System.nanoTime();
                        ProducerRecord<String, String> newRecord = new ProducerRecord<>(TOPIC, textList.get(i));
                        producer.send(newRecord);
                        //System.out.println(System.nanoTime() - cur);
                        while ((System.nanoTime() - cur) < interval) {}
                    }
                    //}
                    System.out.println("size:"+String.valueOf(textList.size()));
                    System.out.println("time:"+String.valueOf((System.nanoTime() - start)/1000000));
                    System.out.println("interval:"+String.valueOf((System.nanoTime() - start)/(textList.size())));
                    textList.clear();
                    continue;
                }
                textList.add(sCurrentLine);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if(stream != null) stream.close();
                if(br != null) br.close();
            } catch(IOException ex) {
                ex.printStackTrace();
            }
        }
        producer.close();
        //logger.info("LatencyLog: " + String.valueOf(System.currentTimeMillis() - time));
    }

    public static void main(String[] args) throws InterruptedException {
		String TOPIC = new String();
        String file = new String();
        if (args.length > 0) {	
			TOPIC = args[0];
        	file = args[1];
		}
		new ProducerExample(TOPIC).generate(file);
    }
}
