package ordo;

import java.rmi.RemoteException;  
import java.rmi.server.UnicastRemoteObject;
import map.Mapper;
import formats.Format;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.Naming;

public class WorkerImpl extends UnicastRemoteObject implements Worker{
    
    private static final long serialVersionUID = 1L;
    private String serverAddress;

    public WorkerImpl (String url) throws RemoteException{
    	super();
    	this.serverAddress = url;
    }
    public void runMap (Mapper m, Format reader, Format writer, CallBack cb) throws RemoteException{
        TaskExecuter mapRunner = new TaskExecuter(m, reader, writer, cb);
        mapRunner.start();
    }
    public String getServerAddress() {
		return this.serverAddress;
    }
    // Initializes a WorkerImpl instance and bounds it to the RMI registry 	
    public static void main(String args[]) {
        int port;
    	try {
    		port = Integer.parseInt(args[1]);
    		System.out.println(" Launching the registry");

    	    Registry registry = LocateRegistry.createRegistry(port);

            Naming.rebind("//"+args[0]+":"+ port + "/worker", new WorkerImpl("//"+args[0]+":"+port+ "/worker"));

    		System.out.println("Worker bound in registry");
    	} catch (NumberFormatException e) {
    		System.out.println(" Usage: java WorkerImpl <machine name> <port number>");
    	} catch (Exception ex) {
    			System.out.println(" Probably the registry is already running ...");
                ex.printStackTrace();
    	}

    }
}
