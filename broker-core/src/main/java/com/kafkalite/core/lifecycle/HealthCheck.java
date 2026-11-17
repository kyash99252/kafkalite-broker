package com.kafkalite.core.lifecycle;

import java.net.InetSocketAddress;
import java.net.Socket;

public class HealthCheck {
    private HealthCheck() {
    }

    public static void main(String[] args) {
        int port = Integer.parseInt(System.getenv().getOrDefault("BROKER_PORT", "9092"));
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", port), 1500);
            System.exit(0);
        } catch (Exception e) {
            System.exit(1);
        }
    }
}
