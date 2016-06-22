/**
 * Created by erikrisinger on 5/2/16.
 */

import java.io.*;
import org.json.simple.*;
import java.net.Socket;
import java.util.*;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class SocketProducerRunnable implements Runnable{

    protected Socket clientSocket = null;
    private int userID;

    public SocketProducerRunnable(Socket clientSocket, int id) {
        this.clientSocket = clientSocket;
        this.userID = id;
    }

    public void run() {
        //Kafka Producer code
        long events = 10000;
        Random rnd = new Random();

        Properties props = new Properties();
        props.put("metadata.broker.list", "localhost:9092,localhost:9093,localhost:9094");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
//            props.put("partitioner.class", "example.producer.SimplePartitioner");
        props.put("request.required.acks", "1");
        props.put("retry.backoff.ms", "500");

        ProducerConfig config = new ProducerConfig(props);

        Producer<String, String> producer = new Producer<String, String>(config);

        System.out.println("CONNECTED");

        try {
            BufferedReader input = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
//            OutputStream output = clientSocket.getOutputStream();

            long time = System.currentTimeMillis();
            long latestTime = time;

            String inputLine;
            JSONParser parser = new JSONParser();
            Map obj = null;
            Map jsonData = null;

            while ((inputLine = input.readLine()) != null) {
//                System.out.println(inputLine);
//                JSONParser parser = new JSONParser();
//                Map obj = null;
//                Map jsonData = null;

                try {
                    obj = (Map)parser.parse(inputLine, new ContainerFactory(){
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

                jsonData = (Map)obj.get("data");


                long checkTime = (long)jsonData.get("t");
                if (checkTime > latestTime){
                    latestTime = checkTime;


                    KeyedMessage<String, String> data = new KeyedMessage<String, String>("sensor-message", inputLine);
//                    System.out.println("message: |" + data.message() + "|");
                    producer.send(data);

                    data = new KeyedMessage<String, String>("user_" + userID, inputLine);
//                    System.out.println("message: |" + data.message() + "|");
                    producer.send(data);
                }
            }
            producer.close();
//            output.close();
            input.close();
            System.out.println("Request processed: " + time);
        } catch (IOException e) {
            //report exception somewhere.
            e.printStackTrace();
        }
    }
}
