package ru.bmstu.distrib.request;

import java.util.List;
import java.util.Random;

/**
 * @author vlad333rrty
 */
public class RequestSendStrategy {
    private final List<String> availableHosts;
    private final Random ran;

    public RequestSendStrategy(List<String> availableHosts) {
        this.availableHosts = availableHosts;
        this.ran = new Random();
    }

    public String getAddress() {
        return availableHosts.get(ran.nextInt(availableHosts.size()));
    }
}
