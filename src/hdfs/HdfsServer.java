package hdfs;

import java.rmi.*;

public interface HdfsServer extends Remote {
    public static final short DEFAULT_SERVER_PORT = 4000;
    public static final int DEFAULT_BLOCK_SIZE = 16;
    public static final int MAX_STRING_LENGTH = 1024;

    enum hdfsOpenMode {R, W};

    public String getAddress() throws RemoteException;
    public short getPort() throws RemoteException;

}
