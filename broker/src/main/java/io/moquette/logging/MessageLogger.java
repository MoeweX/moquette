package io.moquette.logging;

import io.moquette.delaygrouping.peering.messaging.PeerMessagePublish;
import io.netty.handler.codec.mqtt.MqttPublishMessage;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class MessageLogger {
    private final LinkedBlockingQueue<LogMessage> messages = new LinkedBlockingQueue<>();
    private File outputFile;
    private AtomicBoolean isRunning = new AtomicBoolean(false);
    private Thread worker;

    public MessageLogger(String clientId) {
        this.outputFile = new File("results/" + clientId.replaceAll("[/]+", "|") + "_log.csv");
        run();
    }

    private void run() {
        isRunning.set(true);
        worker = new Thread(() -> {

            var messagesToDump = new ArrayList<LogMessage>();
            StringBuilder output = new StringBuilder();

            outputFile.getParentFile().mkdirs();

            try (FileWriter fileWriter = new FileWriter(outputFile, true);
                 BufferedWriter bufferedWriter = new BufferedWriter(fileWriter)) {

                while (isRunning.get() || !messages.isEmpty()) {
                    messages.drainTo(messagesToDump);

                    messagesToDump.forEach(msg -> {
                        output.append(msg.toLogEntry()).append("\n");
                    });

                    bufferedWriter.write(output.toString());

                    messagesToDump.clear();
                    output.setLength(0);

                    bufferedWriter.flush();

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        worker.start();
    }

    public void log(PeerMessagePublish msg) {
        msg.getPublishMessages().forEach(this::log);
    }

    public void log(MqttPublishMessage msg) {
        var logMsg = new NettyMqttPubLogMessage(msg);
        logMsg.timestamp();
        messages.add(logMsg);
    }

    public void log(String topic, byte[] payload) {
        var logMsg = new GenericLogMessage(topic, payload);
        logMsg.timestamp();
        messages.add(logMsg);
    }

    public void shutdown() {
        isRunning.set(false);
        try {
            worker.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
