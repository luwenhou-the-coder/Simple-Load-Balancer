
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LoadBalancer {

    private static final int THREAD_POOL_SIZE = 4;
    private final ServerSocket socket;
    private final DataCenterInstance[] instances;
    private final List cpuUtil = new ArrayList();

    public LoadBalancer(ServerSocket socket, DataCenterInstance[] instances) {
        this.socket = socket;
        this.instances = instances;
        for (int i = 0; i < 3; i++) {   //initialize the cpuUtil list
            this.cpuUtil.add(0.0);
        }
    }

    public static String getRequest(String url) throws InterruptedException, MalformedURLException, IOException {
        //get the cpu utilization of an instance
        URL getUrl = new URL(url);
        HttpURLConnection con = (HttpURLConnection) getUrl.openConnection();

        int counter = 0;
        while (con.getResponseCode() != 200 && counter < 30) {
            Thread.sleep(1000);
            con = (HttpURLConnection) getUrl.openConnection();
            counter++;
        }

        BufferedReader reader = new BufferedReader(
                new InputStreamReader(con.getInputStream()));
        String inputLine;
        StringBuffer response = new StringBuffer();

        while ((inputLine = reader.readLine()) != null) {
            response.append(inputLine);
        }
        reader.close();
        con.disconnect();
        //print result
        return response.toString();
    }

    // Complete this function
    public void start() throws IOException, InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        while (true) {
            for (int i = 0; i < 3; i++) {   //first give a round robin
                Runnable requestHandler = new RequestHandler(socket.accept(), instances[i]);
                executorService.execute(requestHandler);
            }
            for (int i = 0; i < 3; i++) {   //check the cpu utilization and send request to the lowest one
                String cpuUrl = instances[i].getUrl() + ":8080/info/cpu";
                String cpuResponse = getRequest(cpuUrl);
                String cpuUtilLine = cpuResponse.substring(cpuResponse.indexOf("<body>") + 6, cpuResponse.indexOf("</body>"));
                while (cpuUtilLine.equals("")) {
                    cpuResponse = getRequest(cpuUrl);
                    cpuUtilLine = cpuResponse.substring(cpuResponse.indexOf("<body>") + 6, cpuResponse.indexOf("</body>"));
                }
                cpuUtil.set(i, Double.parseDouble(cpuUtilLine));
            }
            int min = cpuUtil.indexOf(Collections.min(cpuUtil));    //get the lowest
            Runnable requestHandler = new RequestHandler(socket.accept(), instances[min]);  //send request to it
            executorService.execute(requestHandler);
            for (int i = 0; i < 3; i++) {   //another round robin, to set the interval of each cpu check
                requestHandler = new RequestHandler(socket.accept(), instances[i]);
                executorService.execute(requestHandler);
            }
        }
    }

}
