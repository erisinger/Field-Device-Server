import java.io.*;
import java.util.*;

import java.net.ServerSocket;
import java.net.Socket;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.json.simple.parser.ContainerFactory;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 * Created by erikrisinger on 3/11/16.
 */


public class FieldDeviceInterfaceServer {

    public static void main (String[] args) throws IOException {
        Socket clientSocket = null;
        int portNumber = 9999;
        int userID = -1; //NEEDS TO BE SET unless handshake is implemented
        ServerSocket serverSocket = null;
        BufferedReader input;
        BufferedOutputStream output;

        boolean running = true;

        serverSocket = new ServerSocket(portNumber);

        while (running) {
            try {
                clientSocket = serverSocket.accept();
                System.out.println("ACCEPTED CLIENT");

                //handshake with field device
                input = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
//                output = new BufferedOutputStream(clientSocket.getOutputStream());
//
//                String[] handshake = input.readLine().split(",");
//
//                //capture user ID and acknowledge
//                if ("ID".equals(handshake[0])){
//                    userID = Integer.parseInt(handshake[1]);
//                    output.write(("ACK," + userID + "\n").getBytes());
//                    output.flush();
//                }
//                else {
//                    System.out.println("handshake from field device failed");
//                }
            } catch (IOException e) {
                System.out.println("Exception caught when trying to listen on port "
                        + portNumber + " or listening for a connection");
                System.out.println(e.getMessage());
            }

            //check for valid user ID
//            if (userID == -1) {
//                System.out.println("bad user ID: handshake with field device failed");
//                return;
//            }

            //all went well -- launch new server thread and return to accept() loop
            new Thread(new SocketProducerRunnable(clientSocket, userID)).start();
        }
        try {
            serverSocket.close();
        } catch (IOException e) {

        }
    }
}