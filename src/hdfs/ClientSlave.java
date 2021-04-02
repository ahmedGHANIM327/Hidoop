package hdfs;

import formats.*;
import formats.Format.Type;

import java.net.*;
import java.io.*;
import java.util.concurrent.*;

public class ClientSlave extends Thread {

    enum CmdType {
        WR, RD, DL
    };

    static final int QUEUE_CAPACITY = 4096;

    private CmdType cmd;
    private Socket socket;
    private String name;
    private Type type;
    private BlockingQueue<KV> queue;
    private HdfsIO io;

    public ClientSlave(String address, short port, CmdType cmd, String name, Format.Type type) {
        this.cmd = cmd;
        this.name = name;
        this.type = type;
        this.queue = new LinkedBlockingQueue<KV>(QUEUE_CAPACITY);
        this.io = new HdfsIO();
        try {
            this.socket = new Socket(address, port);
        } catch (Exception e) {
            System.out.println("Erreur");
            System.exit(0);
            e.printStackTrace();
        }
    }

    public void put(KV kv) throws InterruptedException {
        this.queue.put(kv);
    }

    public KV take() throws InterruptedException {
        return this.queue.take();
    }

    public void run() {
        switch (cmd) {
            case WR :
                try {
                    OutputStream os = socket.getOutputStream();
                    io.writeEnum(os, CmdType.WR);
                    io.writeString(os, name);
                    io.writeEnum(os, this.type);
                    os.flush();
                    for (KV kv = this.queue.take(); kv.k != null; kv = this.queue.take()) {
                        io.writeString(os, kv.k);
                        io.writeString(os, kv.v);
                        os.flush();
                    }
                    io.writeInt(os, 0);
                    os.close();
                    socket.close();
                } catch (Exception e){
                    System.out.println("Error while sending file " + name);
                    e.printStackTrace();
                    System.exit(1);
                }
                break;
            case RD : 
                try {
                    OutputStream os = socket.getOutputStream();
                    io.writeEnum(os, CmdType.RD);
                    io.writeString(os, name);
                    io.writeEnum(os, this.type);
                    os.flush();

                    InputStream is = socket.getInputStream();
 
                    while (true) {
                        // key
                        String key = io.readString(is);
                        if (key.length() == 0) {
                            break;
                        }

                        // value
                        String value = io.readString(is);

                        this.queue.put(new KV(key, value));
                    }
                    this.queue.put(new KV(null, null));
                    os.close();
                    is.close();
                    socket.close();
                } catch (Exception e){
                    System.out.println("Erreur lecture");
                    e.printStackTrace();
                    System.exit(0);
                }
            
            
            
                        break;
            case DL : 
                try {
                    OutputStream os = socket.getOutputStream();
                    io.writeEnum(os, CmdType.DL);
                    io.writeString(os, name);
                    io.writeEnum(os, this.type);
                    os.close();
                    socket.close();
                } catch (Exception e){
                    System.out.println("Erreur");
                    e.printStackTrace();
                    System.exit(0);
                }
                break;
            
        }
        


    }
}
