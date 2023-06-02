package ru.bmstu.distrib;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import db.App;
import db.instructions.SelectInstruction;
import db.transaction.Transaction;
import jdk.jshell.execution.Util;
import ru.bmstu.distrib.pojo.DeleteInstructionPojo;
import ru.bmstu.distrib.pojo.InsertInstructionPojo;
import ru.bmstu.distrib.pojo.InstructionPojo;
import ru.bmstu.distrib.pojo.OperationResult;
import ru.bmstu.distrib.pojo.SelectInstructionPojo;

/**
 * @author vlad333rrty
 */
public class RequestProcessor {
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final App app;
    private final Executor executor = Executors.newFixedThreadPool(10);

    public RequestProcessor(App app) {
        this.app = app;
    }


    public void tryToPerformTransaction(Socket socket) {
        executor.execute(() -> {
            try {
                tryToPerformTransactionInner(socket);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }
    private void tryToPerformTransactionInner(Socket socket) throws Exception {
        byte[] bytes;
        System.out.println("Trying to read client message");
        try (var in = new DataInputStream(socket.getInputStream());
             var os = new DataOutputStream(socket.getOutputStream()))
        {
            int len = ByteBuffer.wrap(in.readNBytes(4)).getInt();
            bytes = in.readNBytes(len);

            System.out.println("done reading");
            List<InstructionPojo> instructionPojo = objectMapper.readValue(bytes, new TypeReference<>() {});

            Transaction transaction = createTransaction(instructionPojo);
            OperationResult result;
            try {
                app.run(transaction);
                result = OperationResult.OK;
            } catch (Exception e) {
                result = OperationResult.BAD;
            }

            os.write(constructResponse(result, transaction));
            os.flush();
        }
    }

    private byte[] constructResponse(OperationResult result, Transaction transaction) throws JsonProcessingException {
        byte[] resArr = objectMapper.writeValueAsBytes(result);
        String transactionId = transaction.id();

        byte[] arrLenBytes = Utils.intToByteArray(resArr.length);
        byte[] strLenBytes = Utils.intToByteArray(transactionId.length());

        return Utils.concat(Utils.concat(arrLenBytes, strLenBytes), Utils.concat(resArr, transactionId.getBytes()));
    }

    public void doCommitTransaction(Socket socket) throws IOException {
        try (var in = new DataInputStream(socket.getInputStream())) {
            int len = ByteBuffer.wrap(in.readNBytes(4)).getInt();
            String transactionId = new String(in.readNBytes(len));
            app.commitTransaction(transactionId);
        }
    }

    public void rollbackTransaction(Socket socket) throws IOException {
        try (var in = new DataInputStream(socket.getInputStream())) {
            int len = ByteBuffer.wrap(in.readNBytes(4)).getInt();
            String transactionId = new String(in.readNBytes(len));
            app.rollbackUncommittedTransaction(transactionId);
        }
    }

    private Transaction createTransaction(List<InstructionPojo> instructionsPojo) throws IOException {
        Transaction.Builder builder = new Transaction.Builder();
        for (InstructionPojo instruction : instructionsPojo) {
            switch (instruction.type()) {
                case DELETE:
                    DeleteInstructionPojo deleteInstructionPojo = objectMapper
                            .readValue(instruction.data(), DeleteInstructionPojo.class);
                    builder.addInstruction(deleteInstructionPojo.toDeleteInstruction());
                    break;
                case INSERT:
                    InsertInstructionPojo instructionPojo = objectMapper
                            .readValue(instruction.data(), InsertInstructionPojo.class);
                    builder.addInstruction(instructionPojo.toInsertInstruction());
                case CREATE:
                    break;
                case UPDATE:
                    break;
                case SELECT:
                    SelectInstructionPojo selectInstructionPojo = objectMapper
                            .readValue(instruction.data(), SelectInstructionPojo.class);
                    builder.addInstruction(selectInstructionPojo.toSelectInstruction());
                    break;
            }
        }

        return builder.build();
    }
}
