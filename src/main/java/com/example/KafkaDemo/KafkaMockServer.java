package com.example.KafkaDemo;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.UnknownHostException;
import java.util.Objects;
import java.util.Properties;
import java.util.Random;

@Service
@Slf4j
public class KafkaMockServer {

    private Random randPortGen = new Random(System.currentTimeMillis());
    private KafkaServer kafkaServer;
    private int zkLocalPort;
    Properties kafkaProperties = new Properties();
    Properties zkProperties = new Properties();

    @PostConstruct
    public void init(){
        try {
            zkProperties.load(Objects.requireNonNull(this.getClass().getClassLoader().getResourceAsStream("zookeeper.properties")));
            kafkaProperties.load(Objects.requireNonNull(this.getClass().getClassLoader().getResourceAsStream("kafka-server.properties")));

            Runtime.getRuntime().addShutdownHook(new Thread(KafkaMockServer::deleteTargetDir));
            startKafkaServer();
        } catch (UnknownHostException e) {
            log.info("Error getting the value of localhost. Proceeding with 'localhost'.");
        } catch (IOException e) {
            log.info("load properties failed.");
        }
    }

    private boolean startKafkaServer() {
        try {
            ZookeeperServer zookeeper;
            while (true) {
                try {
                    zkLocalPort = getNextPort();

                    zkProperties.setProperty("clientPort", Integer.toString(zkLocalPort));
                    zookeeper = new ZookeeperServer(zkProperties);
                    break;
                } catch (BindException bindEx) {
                }
            }
            log.info("ZooKeeper instance is successfully started on port " + zkLocalPort);

            Thread.sleep(2000);
            // override the Zookeeper url.
            kafkaProperties.setProperty("zookeeper.connect", getZkUrl());
            while (true) {
//                kafkaLocalPort = getNextPort();
                // override the Kafka server port
                kafkaProperties.setProperty("port", "9092");
                kafkaServer = new KafkaServer(kafkaProperties);
                try {
                    kafkaServer.start();
                    break;
                } catch (BindException bindEx) {
                    // let's try another port.
                }
            }
            log.info("Kafka Server is successfully started on port 9092");
            return true;

        } catch (Exception e) {
            log.info("Error starting the Kafka Server." + e.getLocalizedMessage());
            return false;
        }
    }

    public void prepare() {
        boolean startStatus = startKafkaServer();
        if (!startStatus) {
            throw new RuntimeException("Error starting the server!");
        }
        try {
            Thread.sleep(3 * 1000);   // add this sleep time to
            // ensure that the server is fully started before proceeding with tests.
        } catch (InterruptedException e) {
            // ignore
        }
        log.info("Completed the prepare phase.");
    }

    public void tearDown() {
        log.info("Shutting down the kafka Server.");
        kafkaServer.stop();
        log.info("Completed the tearDown phase.");
    }

    private synchronized int getNextPort() {
        // generate a random port number between 49152 and 65535
        return randPortGen.nextInt(65535 - 49152) + 49152;
    }

    public String getZkUrl(){
        return "127.0.0.1" + ":" + zkLocalPort;
    }

    public String getKafkaServerUrl(){
        return "127.0.0.1" + ":" + 9092;
    }

    public String getKafkaLogDir() {
        return kafkaProperties.getProperty("log.dirs");
    }

    private static void deleteTargetDir() {
        String targetDir = "target";
        int i = 0;
        while (i++ < 3) {
            try {
                FileUtils.deleteDirectory(new File(targetDir));
            } catch (IOException e) {
                log.info(e.getLocalizedMessage());
            }
            File targetFileDir = new File(targetDir);
            if (!targetFileDir.exists()) {
                break;
            }
        }
    }

}
