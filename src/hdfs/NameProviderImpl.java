package hdfs;

import java.rmi.*;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.*;
import java.util.*;

public class NameProviderImpl extends UnicastRemoteObject implements NameProvider {
    static final long serialVersionUID = -1;

    private List<HdfsFile> files;
    private List<HdfsServer> servers;

    public NameProviderImpl() throws java.rmi.RemoteException {
        this.files = new ArrayList<HdfsFile>();
        this.servers = new ArrayList<HdfsServer>();
    }

    public void addFile(HdfsFile file) throws RemoteException {
        files.add(file);
    }

    public void removeFile(String nameFile) throws RemoteException{
        for(int i = 0; i < files.size(); i++) {
            if (nameFile.equals(files.get(i).getName())) {
                files.remove(i);
                break;
            }
        }
    }

    public Boolean existFile(String nameFile) throws RemoteException {
        for(int i = 0; i < files.size(); i++) {
            if (nameFile.equals(files.get(i).getName())) {
                return true;
            }
        }
        return false;
    }

    public HdfsFile getFile(String nameFile) throws RemoteException {
        for(int i = 0; i < files.size(); i++) {
            if (nameFile.equals(files.get(i).getName())) {
                return files.get(i);
            }
        }
        return null;
    }

    public List<HdfsServer> getServers() throws RemoteException {
        return servers;
    }

    public void addServer(HdfsServer server) throws RemoteException {
        servers.add(server);
    }

    public static void main(String args[]) {
		try {
            HdfsConfig cfg = new HdfsConfig();
			System.out.println(" Launching the registry");
            Registry registry = LocateRegistry.createRegistry(cfg.getProviderPort());
			Naming.rebind("//"+cfg.getProviderAddress()+":"+cfg.getProviderPort()+"/NameProvider", new NameProviderImpl());
			System.out.println("NameProvider bound in registry");
		} catch (Exception e) {
			e.printStackTrace();
        }
	}

}