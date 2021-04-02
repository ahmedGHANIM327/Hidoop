package hdfs;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;

public class HdfsServerRun {
    static HdfsServerImpl hdfsserver;

    public static void main(String[] args) throws RemoteException, MalformedURLException, NotBoundException {
        // java src.HdfsServerRun nom port identifiant
        HdfsConfig cfg = new HdfsConfig();
        short port_server = cfg.getServerPort();

        if (args.length < 1) {
            System.out.println("usage: HdfsServerRun <address>");
            System.exit(1);
        }

        hdfsserver = new HdfsServerImpl(args[0],port_server);

        String address_namenode = cfg.getProviderAddress();
        short port_namenode = cfg.getProviderPort();
        // Connexion au namenode
        NameProvider provider = (NameProvider) Naming.lookup("//"+address_namenode+":"+port_namenode+"/NameProvider");
        provider.addServer(hdfsserver);
        hdfsserver.run();


    }
}
