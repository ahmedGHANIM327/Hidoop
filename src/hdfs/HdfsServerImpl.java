package hdfs;

import java.rmi.*;
import java.rmi.server.*;


import java.net.*;

public class HdfsServerImpl extends UnicastRemoteObject implements HdfsServer, Runnable {

    static final long serialVersionUID = -1;

    // Numero du port du serveur 
    private String address;
    private short port;

    public HdfsServerImpl(String address, short port) throws RemoteException {
        this.address = address;
        this.port = port;
    }

    public String getAddress() throws RemoteException {
        return this.address;
    }

    public short getPort() throws RemoteException {
        return this.port;
    }

    public void run() {
        ServerSocket ss;
		try {
			System.out.println("Listenning for new demands ...");
			ss = new ServerSocket(port);
			while (true){
				new HdfsServerSlave(ss.accept()).start();
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
}
