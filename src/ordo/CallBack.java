package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.concurrent.Semaphore;

public interface CallBack extends Remote {
	public void notifyMapDone() throws RemoteException;
	public int getTasksDone() throws RemoteException;
	public boolean AllMapsDone() throws RemoteException;
}
