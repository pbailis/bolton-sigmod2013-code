package edu.berkeley.lipstick.simpleserver;

import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

public class SimpleServer {
    public static final int DEFAULT_PORT = 7911;

    private void start(int socket) {
        try {
            TServerSocket serverTransport = new TServerSocket(socket);

            SimpleServerService.Processor processor = new SimpleServerService.Processor(new SimpleServerServiceImpl());

            TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).
                    processor(processor));
            server.serve();
        } catch (TTransportException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        int socket = DEFAULT_PORT;

        if(args.length < 1) {
            System.out.println("No port specified; using default: "+DEFAULT_PORT);
        }
        else {
            System.out.println("Starting server on port: "+args[0]);
            socket = Integer.parseInt(args[0]);
        }

        SimpleServer srv = new SimpleServer();
        srv.start(socket);
    }
}