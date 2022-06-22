package com.slk.kafka.manager;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.transaction.annotation.EnableTransactionManagement;

/**
 * @author Sherlock
 * @date 2022/6/19 0019 21:17
 * @describe
 **/
@SpringBootApplication(scanBasePackages = "com.slk")
@EnableTransactionManagement
@EnableJpaRepositories
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class,args);
    }
}
