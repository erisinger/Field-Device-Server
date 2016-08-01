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
    private int userID;

    public SocketProducerRunnable(Socket clientSocket, int id) {
        this.clientSocket = clientSocket;
        this.userID = id;
    }

    public void run() {

        try {
            BufferedReader input = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
            OutputStream output = clientSocket.getOutputStream();

            System.out.println("CONNECTED");

            configureProducer();
            configureConsumer();

            //begin reading in sensor data, emitting to Kafka
            String inputLine;
            while ((inputLine = input.readLine()) != null) {
                if (inputLine.split(",").length != 2) {
                    //emit to one or more Kafka topics
                    emitStringWithTopic(inputLine, "sensor-message");
                    emitStringWithTopic(inputLine, "user_" + userID);
                }
            }

            //clean up
            producer.close();
            output.close();
            input.close();
            System.out.println("Request processed: " + System.currentTimeMillis());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void emitStringWithTopic(String str, String topic){
        KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, str);
        producer.send(data);
    }

    private void configureProducer(){
        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092,localhost:9093,localhost:9094");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
//        props.put("partitioner.class", "example.producer.SimplePartitioner");
        props.put("request.required.acks", "1");
        props.put("retry.backoff.ms", "500");

        ProducerConfig config = new ProducerConfig(props);

        producer = new Producer<String, String>(config);
    }

    private void configureConsumer(){
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test" + System.currentTimeMillis());
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //launch runnable
        ArrayList<String> topicsList = new ArrayList<>();
        topicsList.add("user_1");

        KafkaConsumerRunnable consumerRunnable = new KafkaConsumerRunnable(props, topicsList, clientSocket);
        Thread consumerThread = new Thread(consumerRunnable);
        consumerThread.start();

//        try {
//            consumerThread.join();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
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

                        if ("SENSOR_STEP".equals(obj.get("sensor_type"))) {
//                            Map data = (Map)obj.get("data");
//                            String outString = (String)data.get("s");

                            //this line is for testing only...
                            String outString = "step at " + System.currentTimeMillis();

                            if (outString != null) {

                                System.out.println(outString);

                                output.write(outString + "\n");
                                output.flush();
                            }
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
