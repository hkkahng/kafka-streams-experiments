package com.fico.kafkastreams.showcase;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fico.kafkastreams.showcase.serdes.SerdesProvider;
import com.fico.kafkastreams.showcase.utils.JsonUtils;
import com.fico.kafkastreams.showcase.utils.UuidKeyUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;

@Configuration
@EnableAsync
public class StreamsExpConfiguration {

    @Bean
    public ObjectMapper objectMapper() {
        return JsonUtils.getObjectMapper();
    }

    @Bean
    public SerdesProvider serdesProvider() {
        return new SerdesProvider(objectMapper());
    }

    @Bean
    public StreamsExpProperties streamsExpProperties() {
        return new StreamsExpProperties();
    }

    @Bean
    public UuidKeyUtils uuidKeyUtils() {
        return new UuidKeyUtils();
    }
}
