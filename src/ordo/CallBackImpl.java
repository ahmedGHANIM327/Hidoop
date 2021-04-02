package ordo;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.Semaphore;

public class CallBackImpl extends UnicastRemoteObject implements CallBack {
    private static final long serialVersionUID = 1L;
    private int tasksDone;    
	private int tasksWaiting;

    public CallBackImpl(int nbTasks) throws RemoteException {
    	tasksWaiting = nbTasks;
    	tasksDone = 0;
    }

    /** Count the number of tasks that are done
     */
    public synchronized void notifyMapDone() throws RemoteException {
        this.tasksDone++;
    }

    /**
     * @return number of tasks done
     * @throws RemoteException
     */
	public int getTasksDone() throws RemoteException{
        return this.tasksDone;
    }
	/**
	 * @return true if all workers finished their maps
	 */
	public boolean AllMapsDone() throws RemoteException {
		return tasksWaiting == tasksDone;
	}
    
}
