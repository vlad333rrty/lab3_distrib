import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import ru.bmstu.distrib.Utils;
import ru.bmstu.distrib.pojo.AttributeConditionPojo;
import ru.bmstu.distrib.pojo.DeleteInstructionPojo;
import ru.bmstu.distrib.pojo.InsertInstructionPojo;
import ru.bmstu.distrib.pojo.InstructionPojo;
import ru.bmstu.distrib.pojo.InstructionPojoType;
import ru.bmstu.distrib.pojo.OperationResult;
import ru.bmstu.distrib.pojo.SelectInstructionPojo;
import ru.bmstu.distrib.pojo.ValuesTuplePojo;

/**
 * @author vlad333rrty
 */
public class Client {
    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("start");
        PrepareCommitResult r1 = processForHost("vlad333rrty.sas.yp-c.yandex.net");
        PrepareCommitResult r2 = processForHost("localhost");

        if (r1.operationResult == OperationResult.OK && r2.operationResult == OperationResult.OK) {
            sendShouldCommit(r1.transactionId, "vlad333rrty.sas.yp-c.yandex.net", true);
            sendShouldCommit(r2.transactionId, "localhost", true);
        } else {
            sendShouldCommit(r1.transactionId, "vlad333rrty.sas.yp-c.yandex.net", false);
            sendShouldCommit(r2.transactionId, "localhost", false);
        }
    }

    private static PrepareCommitResult processForHost(String host) throws IOException, InterruptedException {
        return sendPrepareCommit(host);
    }

    public static void sendShouldCommit(String transactionId, String host, boolean shouldCommit) throws IOException {
        if (shouldCommit) {
            try (Socket socket = new Socket(host, 8081);
                 var os = new DataOutputStream(socket.getOutputStream()))
            {
                byte[] bytes = Utils.concat(Utils.intToByteArray(transactionId.length()), transactionId.getBytes());
                os.write(bytes);
                os.flush();
            }
        } else {
            try (Socket socket = new Socket(host, 8082);
                 var os = new DataOutputStream(socket.getOutputStream()))
            {
                byte[] bytes = Utils.concat(Utils.intToByteArray(transactionId.length()), transactionId.getBytes());
                os.write(bytes);
                os.flush();
            }
        }
    }


    public static PrepareCommitResult sendPrepareCommit(String host) throws IOException, InterruptedException {
        ObjectMapper objectMapper = new ObjectMapper();
        try (Socket socket = new Socket(host, 8080);
             var os = new DataOutputStream(socket.getOutputStream());
             var in = new DataInputStream(socket.getInputStream()))
        {
            byte[] bytes = objectMapper.writeValueAsBytes(getInstructionPojo());
            int len = bytes.length;
            byte[] lenAndBytes = Utils.concat(Utils.intToByteArray(len), bytes);
            os.write(lenAndBytes);
            os.flush();

            waitForResponse();
            int dataBytesLen = ByteBuffer.wrap(in.readNBytes(4)).getInt();
            int transactionIdLen = ByteBuffer.wrap(in.readNBytes(4)).getInt();

            byte[] res = in.readNBytes(dataBytesLen);
            String transactionId = new String(in.readNBytes(transactionIdLen));
            OperationResult result = objectMapper.readValue(res, OperationResult.class);

            return new PrepareCommitResult(result, transactionId);
        }
    }

    // todo we cannot detect that pod is dead!!!
    private static void waitForResponse() throws InterruptedException {
        Thread.sleep(1000);
    }

    private static List<InstructionPojo> getInstructionPojo() throws JsonProcessingException {
        ObjectMapper objectMapper = new ObjectMapper();

        InsertInstructionPojo instructionPojo = new InsertInstructionPojo(
                "test",
                List.of(new ValuesTuplePojo(List.of(1337, 2)))
        );
        var select = new SelectInstructionPojo("test", List.of("x"), List.of());
        byte[] data = objectMapper.writeValueAsBytes(instructionPojo);
        byte[] selectBytes = objectMapper.writeValueAsBytes(select);
        return List.of(
                new InstructionPojo(InstructionPojoType.INSERT, data),
                new InstructionPojo(InstructionPojoType.SELECT, selectBytes)
        );
    }

    private record PrepareCommitResult(OperationResult operationResult, String transactionId) {}
}
