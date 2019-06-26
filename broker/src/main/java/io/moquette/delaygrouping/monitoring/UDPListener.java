package io.moquette.delaygrouping.monitoring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;

public class UDPListener extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(UDPListener.class);
    private DatagramSocket socket;
    private boolean running;
    private DatagramPacket rcvPacket;
    private LinkedBlockingQueue<Measurement> outputQueue;
    private LinkedBlockingQueue<InetSocketAddress> inputQueue;
    private Set<String> pendingRequests = new HashSet<>();

    public UDPListener(int listenPort, LinkedBlockingQueue<InetSocketAddress> inputQueue, LinkedBlockingQueue<Measurement> outputQueue) throws SocketException {
        this.outputQueue = outputQueue;
        this.inputQueue = inputQueue;
        this.socket = new DatagramSocket(listenPort);
        this.socket.setSoTimeout(50);
        byte[] rcvBuffer = new byte[256];
        this.rcvPacket = new DatagramPacket(rcvBuffer, rcvBuffer.length);
    }

    @Override
    public void run() {
        running = true;
        try {
            while (running) {

                try {
                    // Handle incoming packets (blocks for only 50ms)
                    socket.receive(rcvPacket);
                    String sendString = handleMessage(rcvPacket);

                    if (sendString != null) {
                        byte[] sendBuffer = sendString.getBytes(StandardCharsets.UTF_8);
                        socket.send(new DatagramPacket(sendBuffer, sendBuffer.length, rcvPacket.getAddress(), rcvPacket.getPort()));
                    }
                } catch (SocketTimeoutException ignored) {}


                // Check if input queue has outgoing packets to send
                InetSocketAddress pingRequest = inputQueue.poll();
                if (pingRequest != null) {
                    long epochNano = getEpochNano();

                    // Save unique id
                    String id = UUID.randomUUID().toString();
                    pendingRequests.add(id);

                    byte[] sendBuffer = ("PING;" + epochNano + ";" + id).getBytes(StandardCharsets.UTF_8);
                    socket.send(new DatagramPacket(sendBuffer, sendBuffer.length, pingRequest.getAddress(), pingRequest.getPort()));
                }
            }
        } catch (IOException ex) {
            LOG.error("Got exception: {}", ex);
        }
    }

    private String handleMessage(DatagramPacket rcvPacket) {
        String rcvMsg = new String(rcvPacket.getData(), 0, rcvPacket.getLength());

        String[] fields = rcvMsg.split(";");
        String type = fields[0];
        Long sentEpochNano = Long.valueOf(fields[1]);
        String id = fields[2];

        switch (type) {

            case "PING":
                return rcvMsg.replace("PING", "PONG");

            case "PONG":
                // calculate and save delay
                if (pendingRequests.remove(id)) {
                    double delay = ((getEpochNano() - sentEpochNano) / 2d) / 1_000_000d;
                    outputQueue.add(new Measurement(new InetSocketAddress(rcvPacket.getAddress(), rcvPacket.getPort()), delay));
                } else {
                    LOG.warn("Received unsolicited message from {}:{} ", rcvPacket.getAddress(), rcvPacket.getPort());
                }
                return null;

            default:
                LOG.warn("Received unexpected message: {}", rcvMsg);
                return null;
        }

    }

    private long getEpochNano() {
        Instant now = Instant.now();
        return (now.getEpochSecond() * 1_000_000_000L) + now.getNano();
    }

    public void shutdown() {
        this.running = false;
    }

    public class Measurement {
        public InetSocketAddress peerAddress;
        public double delayValue;

        public Measurement(InetSocketAddress peerAddress, double delayValue) {
            this.peerAddress = peerAddress;
            this.delayValue = delayValue;
        }
    }
}
