package com.example.KafkaDemo;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;

import java.io.IOException;
import java.util.Properties;

@Slf4j
public class ZookeeperServer {
    private ZooKeeperServerMain zooKeeperServer;

    public ZookeeperServer(Properties zkProperties) throws IOException {
        QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
        try {
            quorumConfiguration.parseProperties(zkProperties);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }

        zooKeeperServer = new ZooKeeperServerMain();
        final ServerConfig configuration = new ServerConfig();
        configuration.readFrom(quorumConfiguration);

        new Thread(() -> {
            try {
                zooKeeperServer.runFromConfig(configuration);
            } catch (IOException e) {
                log.error("Zookeeper startup failed. {}", e.getLocalizedMessage());
            }
        }).start();
    }
}
