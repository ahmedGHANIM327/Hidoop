package ordo;

import java.rmi.Naming;
import formats.Format;
import map.Mapper;
import java.io.File;
import java.rmi.RemoteException;

/**Execute 'map' task
 * it's a thread that will 
 * be launched/managed by the worker(Daemon)
 */
public class TaskExecuter extends Thread {
    Mapper m;
	Format reader, writer;
    CallBack cb;

	public TaskExecuter(Mapper m, Format reader, Format writer, CallBack cb) {
		this.m = m;
		this.reader = reader;
		this.writer = writer;
        this.cb = cb;
	}
    public void run(){
        reader.open(Format.OpenMode.R);
        writer.open(Format.OpenMode.W);
		// Lancer la fonction map sur le fragment de fichier
        m.map(reader, writer);
		reader.close();
		writer.close();
        // Utiliser Callback pour prévenir que le traitement est terminé
        try {
			// Indiquer a l'odonnanceur que la tâche est terminée
			cb.notifyMapDone();
			System.out.println("Map task is done.");
		} catch(RemoteException e){
			e.printStackTrace();
			System.out.println("CallBack problem ..!");
		}
    }
}
