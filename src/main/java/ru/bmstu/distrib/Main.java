package ru.bmstu.distrib;

import java.net.http.HttpClient;
import java.util.List;

import db.instructions.AttributesCondition;
import db.instructions.DeleteInstruction;
import db.transaction.Transaction;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.context.annotation.Import;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.bmstu.distrib.pojo.AttributeConditionPojo;
import ru.bmstu.distrib.pojo.OperationResult;
import ru.bmstu.distrib.request.RequestSendStrategy;

/**
 * @author vlad333rrty
 */
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class})
@RestController
@Import({
        DBMSConfiguration.class
})
public class Main {

    @Autowired
    private RequestSendStrategy requestSendStrategy;

    public static void main(String[] args) {
        SpringApplication.run(Main.class, args);
    }

    @GetMapping("/get-message")
    public String getMessage(@RequestParam(value = "name", defaultValue = "world") String name) {
        return "Hello, %s!".formatted(name);
    }

    @GetMapping("/perform-delete")
    public OperationResult performDelete(
            @RequestParam(value = "table_name") String tableName,
            @RequestParam(value = "attributeConditionsPojo") List<AttributeConditionPojo> attributeConditionsPojo)
    {
        List<AttributesCondition> attributesConditions = attributeConditionsPojo.stream()
                .map(x -> new AttributesCondition(x.attributeName(), x.expectedValue()))
                .toList();
        String host = requestSendStrategy.getAddress();

        new Transaction.Builder()
                .addInstruction(new DeleteInstruction(tableName, attributesConditions))
                .build();

        return null;
    }
}
