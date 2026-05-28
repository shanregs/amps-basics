package com.shan.mq.amps.ampsbasics.config;

import com.crankuptheamps.client.DefaultServerChooser;
import com.crankuptheamps.client.HAClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class AmpsConfig {

    @Value("${amps.server.url}")
    private String ampsServerUrl;

    @Value("${amps.client.name}")
    private String clientName;

    @Bean
    public HAClient ampsClient() throws Exception {
        DefaultServerChooser serverChooser = new DefaultServerChooser();
        serverChooser.add(ampsServerUrl);
        log.info("Amps Server URL: " + ampsServerUrl);
        HAClient haClient = new HAClient(clientName);
        haClient.setServerChooser(serverChooser);
        haClient.connectAndLogon();
        log.info("AmpsClient connected and logon successfully");
        return haClient;

    }
}
