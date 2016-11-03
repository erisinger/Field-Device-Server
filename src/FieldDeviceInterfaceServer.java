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
        ServerSocket serverSocket = null;

        boolean running = true;

        serverSocket = new ServerSocket(portNumber);

        while (running) {
            try {
                clientSocket = serverSocket.accept();
                System.out.print("accepted client " + clientSocket.getInetAddress().getHostAddress() + "...");

            } catch (IOException e) {
                System.out.println("Exception caught when trying to listen on port "
                        + portNumber + " or listening for a connection");
                System.out.println(e.getMessage());
            }

            new Thread(new SocketProducerRunnable(clientSocket)).start();
        }
        try {
            serverSocket.close();
        } catch (IOException e) {

        }
    }
}