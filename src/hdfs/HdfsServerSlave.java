package hdfs;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

import formats.Format;
import formats.KV;
import formats.KVFormat;
import formats.LineFormat;
import formats.Format.OpenMode;
import hdfs.ClientSlave.CmdType;

public class HdfsServerSlave extends Thread{

    private Socket socket;
    private HdfsIO io;


    public HdfsServerSlave(Socket socket) {
        this.socket = socket;
        this.io = new HdfsIO();
    }

    public void run() {
        InputStream is = null;
        try {
            is = socket.getInputStream();
            CmdType cmdtype = io.readCmdType(is);
            String name = io.readString(is);
            switch(cmdtype) {
                case DL:
                    System.out.println("Demande de suppression du fichier : "+ name);
                    File file = new File(name);
                    file.delete();
                    System.out.println(name +" a été supprimé.");
                    break;
                case RD:
                    System.out.println("Demande de lecture du fichier : "+ name);
                    Format.Type TypeFile = io.readFmtType(is);

                    Format file_read = null;
                    // Choose the right Format to open the file to read.
                    switch (TypeFile){
                        case KV:
                            file_read = new KVFormat(name);
                            break;
                        case LINE:
                            file_read = new LineFormat(name);
                            break;
                        default:
                            System.out.println("Format inconnu !");
                            System.exit(1);     
                    }

                    file_read.open(OpenMode.R);

                    OutputStream os = socket.getOutputStream();
                    KV kv;
                    while ((kv = file_read.read()) != null) {
                        io.writeString(os, kv.k);
                        io.writeString(os, kv.v);
                        os.flush();
                    }
                    io.writeInt(os, 0);
                    os.close();
                    System.out.println(name + " a été envoyé au client.");
                    break;
                case WR:
                    System.out.println("Demande d'écriture du fichier : " + name);
                    Format.Type TypeFileW = io.readFmtType(is);
                    Format file_write = null;

                    switch (TypeFileW){
                        case KV:
                            file_write = new KVFormat(name);
                            break;
                        case LINE:
                            file_write = new LineFormat(name);
                            break;
                        default:
                            System.out.println("Format inconnue !");
                            System.exit(1);     
                    };
                    file_write.open(OpenMode.W);

                    while (true) {
                        // key
                        String key = io.readString(is);
                        if (key.length() == 0) {
                            break;
                        }
                        // value
                        String value = io.readString(is);
                        file_write.write(new KV(key, value));
                    }

                    file_write.close();
                    System.out.println(name + " a été enregistré");

                    break;
                default :
                    System.out.println("Opération Inconnue !");
                    System.exit(1);  
            }
            is.close();
            socket.close();
        } catch (Exception e) {
            try {
                if (is != null) {
                    is.close();
                }
                socket.close();
            } catch (IOException e2) {
                e2.printStackTrace();
            }
            e.printStackTrace();
        }
	}

}
