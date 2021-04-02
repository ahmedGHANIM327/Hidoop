package hdfs;

import java.rmi.*;
import java.util.List;

public interface NameProvider extends Remote {

    public void addFile(HdfsFile file) throws RemoteException;

    public void removeFile(String nameFile) throws RemoteException;

    public Boolean existFile(String nameFile) throws RemoteException;

    public HdfsFile getFile(String nameFile) throws RemoteException;

    public List<HdfsServer> getServers() throws RemoteException;

    public void addServer(HdfsServer server) throws RemoteException;

}