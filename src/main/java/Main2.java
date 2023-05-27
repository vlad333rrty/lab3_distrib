import java.io.IOException;
import java.util.Scanner;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

/**
 * @author vlad333rrty
 */
public class Main2 {
    public static void main(String[] args) throws IOException {
        try(CloseableHttpClient httpclient = HttpClients.createDefault()) {
            System.out.println("start");
            HttpGet httpget = new HttpGet("http://localhost:8080/get-message");
            Scanner scanner = new Scanner(System.in);
            while (scanner.nextLine().equals("go")) {
                CloseableHttpResponse response = httpclient.execute(httpget);
                System.out.println("Execution complete");
                System.out.println(new String(response.getEntity().getContent().readAllBytes()));
            }
        }
    }
}
