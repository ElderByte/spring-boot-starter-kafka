package com.elderbyte.kafka.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
@SpringBootApplication
public class SpringBootTestApp {

    @Bean
    public ObjectMapper objectMapper(){
        return new ObjectMapper();
    }


    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(SpringBootTestApp.class);
        Environment env = app.run(args).getEnvironment();
    }

}