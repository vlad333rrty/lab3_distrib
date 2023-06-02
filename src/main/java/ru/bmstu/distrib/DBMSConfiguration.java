package ru.bmstu.distrib;

import java.util.List;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.bmstu.distrib.request.RequestSendStrategy;

/**
 * @author vlad333rrty
 */
@Configuration
public class DBMSConfiguration {
    @Bean
    public RequestSendStrategy requestSendStrategy() {
        return new RequestSendStrategy(List.of(
                "localhost",
                "vlad333rrty.sas.yp-c.yandex.net"
        ));
    }
}
