/**
 * Created by erikrisinger on 5/2/16.
 */

import java.io.*;
import java.net.Socket;
import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class SocketProducerRunnable implements Runnable{
    private Producer<String, String> producer;
    protected Socket clientSocket = null;
    private String userID;

    public SocketProducerRunnable(Socket clientSocket) {
        this.clientSocket = clientSocket;
    }

    public void run() {

        try {
            BufferedReader input = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            OutputStream output = clientSocket.getOutputStream();

            System.out.print("connecting...");

            output.write("ID\n".getBytes());
            String handshake = input.readLine();
            if (handshake == null){
                System.out.println("null handshake -- aborting");
                return;
            }

            String[] split = handshake.split(",");

            if (split.length != 2 || !"ID".equals(split[0])){
                System.out.println("invalid handshake: " + handshake);
                return;
            }

            userID = split[1];
            System.out.println("connected with ID " + userID);
            output.write(("ACK," + userID + "\n").getBytes());

            configureProducer();
            configureConsumer();

            //begin reading in sensor data, emitting to Kafka
            String inputLine;
            while ((inputLine = input.readLine()) != null) {

                //failsafe against handshake leaking into Kafka logs -- REPLACE with more effective message validation
                if (inputLine.split(",").length != 2) {
                    //emit to one or more Kafka topics
                    emitStringWithTopic(inputLine, "sensor-message");
                    emitStringWithTopic(inputLine, "user_" + userID);

//                    System.out.println("emitted: " + inputLine);
                }
            }

            //close kafka producer
            producer.close();
            System.out.print("producer closed...");

            //clean up socket stuff
            if (!clientSocket.isClosed()) {
                output.close();
                input.close();
                System.out.print("input and output closed...");
                clientSocket.close();
            }
            System.out.println("socket closed");


            System.out.println("session terminated gracefully at " + System.currentTimeMillis());

        } catch (IOException e) {
            e.printStackTrace();
        } finally {

        }
    }

    private void emitStringWithTopic(String str, String topic){
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, str);
        producer.send(data);
    }

    private void configureProducer(){
        Properties props = new Properties();
//        props.put("metadata.broker.list", "localhost:9092,localhost:9093,localhost:9094");
        props.put("metadata.broker.list", "none.cs.umass.edu:9092,none.cs.umass.edu:9093,none.cs.umass.edu:9094");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
//        props.put("partitioner.class", "example.producer.SimplePartitioner");
        props.put("request.required.acks", "1");
        props.put("retry.backoff.ms", "500");

        ProducerConfig config = new ProducerConfig(props);

        producer = new Producer<String, String>(config);
    }

    private void configureConsumer(){
        Properties props = new Properties();
//        props.put("bootstrap.servers", "localhost:9092");
        props.put("bootstrap.servers", "none.cs.umass.edu:9092,none.cs.umass.edu:9093,none.cs.umass.edu:9094");
        props.put("group.id", "test" + System.currentTimeMillis());
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //launch runnable
        ArrayList<String> topicsList = new ArrayList<>();
        topicsList.add("user_" + userID);
        topicsList.add("flink-analytics");

        KafkaConsumerRunnable consumerRunnable = new KafkaConsumerRunnable(props, topicsList, clientSocket);
        Thread consumerThread = new Thread(consumerRunnable);
        consumerThread.start();
    }

    private class KafkaConsumerRunnable implements Runnable{

        KafkaConsumer<String, String> consumer;
        BufferedWriter output = null;

        public KafkaConsumerRunnable(Properties props, List<String> topics, Socket socket) {
            try {
                this.output = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            } catch (IOException e){
                e.printStackTrace();
            }

            //kafka consumer code
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(topics);
        }

        public void run() {

            JSONParser parser = new JSONParser();
            Map obj = null;

            while (true){

                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    try {
                        try {
                            obj = (Map)parser.parse(record.value(), new ContainerFactory(){
                                public List creatArrayContainer() {
                                    return new LinkedList();
                                }

                                public Map createObjectContainer() {
                                    return new LinkedHashMap();
                                }
                            });
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }

                        if (userID.equals(obj.get("user_id")) && "SENSOR_SERVER_MESSAGE".equals(obj.get("sensor_type"))) {

                            output.write(record.value() + "\n");
                            output.flush();
//                            System.out.println("sent server message " + record.value());

//                            Map data = (Map)obj.get("data");
//                            String outString = (String)data.get("s");

                            //this line is for testing only...
//                            String outString = "step at " + System.currentTimeMillis();
//
//                            if (outString != null) {
//
////                                System.out.println(outString);
//
//                                output.write(outString + "\n");
//                                output.flush();
//                            }
                        }

//                    System.out.println(record.value());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

        }
    }
}