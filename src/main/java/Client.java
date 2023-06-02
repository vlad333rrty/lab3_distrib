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
import ru.bmstu.distrib.pojo.InstructionPojo;
import ru.bmstu.distrib.pojo.InstructionPojoType;
import ru.bmstu.distrib.pojo.OperationResult;
import ru.bmstu.distrib.pojo.SelectInstructionPojo;

/**
 * @author vlad333rrty
 */
public class Client {
    public static void main(String[] args) throws IOException, InterruptedException {
        System.out.println("start");
        processForHost("vlad333rrty.sas.yp-c.yandex.net");
    }

    private static void processForHost(String host) throws IOException, InterruptedException {
        PrepareCommitResult prepareCommitResult = sendPrepareCommit(host);
        sendShouldCommit(prepareCommitResult, host);
    }

    public static void sendShouldCommit(PrepareCommitResult prepareCommitResult, String host) throws IOException {
        String transactionId = prepareCommitResult.transactionId;
        if (prepareCommitResult.operationResult == OperationResult.OK) {
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
        DeleteInstructionPojo op = new DeleteInstructionPojo(
                "test",
                List.of(new AttributeConditionPojo("x", 2))
        );
        var select = new SelectInstructionPojo("test", List.of("x"), List.of());
        byte[] data = objectMapper.writeValueAsBytes(op);
        byte[] selectBytes = objectMapper.writeValueAsBytes(select);
        return List.of(
                new InstructionPojo(InstructionPojoType.SELECT, selectBytes)
        );
    }

    private record PrepareCommitResult(OperationResult operationResult, String transactionId) {}
}
