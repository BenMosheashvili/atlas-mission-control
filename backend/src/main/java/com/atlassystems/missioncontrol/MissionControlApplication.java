package com.atlassystems.missioncontrol;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;
@SpringBootApplication @EnableScheduling
public class MissionControlApplication {
    public static void main(String[] args) { SpringApplication.run(MissionControlApplication.class, args); }
}
