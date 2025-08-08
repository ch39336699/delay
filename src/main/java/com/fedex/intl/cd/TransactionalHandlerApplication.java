package com.fedex.intl.cd;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
@EnableEurekaClient
public class TransactionalHandlerApplication {

  public static void main(String[] args) {
    SpringApplication application = new SpringApplication(TransactionalHandlerApplication.class);
    application.setAddCommandLineProperties(true);
    application.run(args);
  }

}
