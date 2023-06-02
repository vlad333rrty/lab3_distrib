package ru.bmstu.distrib;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import db.App;

/**
 * @author vlad333rrty
 */
public class RequestHandler {
    public static void main(String[] args) throws Exception {
        App app = App.getInstance(Path.of("data/db"));
        RequestProcessor processor = new RequestProcessor(app);

        ExecutorService executor = Executors.newFixedThreadPool(3);

        executor.execute(() -> {
            try (ServerSocket server = new ServerSocket(8080)) {
                while (true) {
                    Socket socket = server.accept();
                    processor.tryToPerformTransaction(socket);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        executor.execute(() -> {
            try (ServerSocket server = new ServerSocket(8081)) {
                while (true) {
                    Socket socket = server.accept();
                    processor.doCommitTransaction(socket);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        executor.execute(() -> {
            try (ServerSocket server = new ServerSocket(8082)) {
                while (true) {
                    Socket socket = server.accept();
                    processor.rollbackTransaction(socket);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        System.out.println("Server started");
        executor.shutdown();
        executor.awaitTermination(24, TimeUnit.HOURS); // todo
    }
}
