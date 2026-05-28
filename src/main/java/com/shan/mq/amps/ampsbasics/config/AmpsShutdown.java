package com.shan.mq.amps.ampsbasics.config;

import com.crankuptheamps.client.HAClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class AmpsShutdown implements ApplicationListener<ContextClosedEvent> {

    private final HAClient ampsClient;

    public AmpsShutdown(HAClient ampsClient) {
        this.ampsClient = ampsClient;
    }

    @Override
    public void onApplicationEvent(ContextClosedEvent event) {
        try{
            log.info("🔌 Disconnecting AMPS client...");
            ampsClient.disconnect();
        }catch (Exception e){
            log.error("Error during AMPS disconnect: " + e.getMessage());
        }

    }

    @Override
    public boolean supportsAsyncExecution() {
        return ApplicationListener.super.supportsAsyncExecution();
    }
}
