package org.myprojects.srvrequestcontrolservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;

@Slf4j
@SpringBootApplication
public class RequestControlApplication {

    @Value("${service.name}")
    private String serviceName;
    @Value("${spring.datasource.url}")
    private String dbUrl;

    public static void main(String[] args) {
        SpringApplication.run(RequestControlApplication.class, args);
    }

    @PostConstruct
    private void startupApplication() {
        log.info("\n=== MAIN SYSTEM VARIABLES START===\n" +
                        "service.name={}\n" +
                        "spring.datasource.url={}\n" +
                        "=== MAIN SYSTEM VARIABLES END===",
                serviceName,
                dbUrl
        );
    }
}
