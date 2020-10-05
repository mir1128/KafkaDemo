package com.example.KafkaDemo;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;

@RestController
@Slf4j
public class KafkaController {

    private KafkaService kafkaService;

    public KafkaController(KafkaService kafkaService) {
        this.kafkaService = kafkaService;
    }

    @GetMapping("/ping")
    public String ping() {
        try {
            String message = kafkaService.sendMessage();
            log.info(message);
            return message;
        } catch (ExecutionException | InterruptedException e) {
            return "false";
        }
    }
}
