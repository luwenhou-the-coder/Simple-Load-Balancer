import java.io.IOException;
import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class LoadBalancer {
	private static final int THREAD_POOL_SIZE = 4;
	private final ServerSocket socket;
	private final DataCenterInstance[] instances;

	public LoadBalancer(ServerSocket socket, DataCenterInstance[] instances) {
		this.socket = socket;
		this.instances = instances;
	}

	// Complete this function
	public void start() throws IOException {
		ExecutorService executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
		int i=0;
		while (true) {
			//in each loop, send to one unique instance
			Runnable requestHandler = new RequestHandler(socket.accept(), instances[i]);
            		executorService.execute(requestHandler);
            		i++;
            		if (i == 3) {
               		 i = 0;
           		 }
		}
	}
}
