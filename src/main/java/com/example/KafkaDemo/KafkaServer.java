package com.example.KafkaDemo;


import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

import java.io.IOException;
import java.util.Properties;

public class KafkaServer {

    public KafkaServerStartable kafka;
    public ZookeeperServer zookeeper;

    public KafkaServer(Properties kafkaProperties) throws IOException, InterruptedException{
        KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);

        //start local kafka broker
        kafka = new KafkaServerStartable(kafkaConfig);
    }

    public void start() throws Exception{
        kafka.startup();
    }

    public void stop(){
        kafka.shutdown();
    }
}
