/* une PROPOSITION de squelette, incomplète et adaptable... */

package hdfs;
import formats.*;
import hdfs.ClientSlave.CmdType;

import java.rmi.*;
import java.util.*;
import java.nio.file.*;

public class HdfsClient {

    private static void usage() {
        System.out.println("Usage: java HdfsClient read <file>");
        System.out.println("Usage: java HdfsClient write <line|kv> <file>");
        System.out.println("Usage: java HdfsClient delete <file>");
    }
	
    public static void HdfsDelete(String hdfsFname) {
        HdfsConfig cfg = new HdfsConfig();
        try {
            NameProvider provider = (NameProvider)Naming.lookup("//"+cfg.getProviderAddress()+":"+cfg.getProviderPort()+"/NameProvider");

            HdfsFile hdfsfile = provider.getFile(hdfsFname);
            Format.Type fmt = hdfsfile.getType();
            Map<String,HdfsServer> servers = hdfsfile.getDistribution();


            int count = servers.size();
            String address;
            ClientSlave slave[] = new ClientSlave[count];
            int iterate = 0;
            for (String key : servers.keySet()) {
                short port = (short) servers.get(key).getPort();
                String name = key;
                address = servers.get(key).getAddress();
                slave[iterate] = new ClientSlave(address, port, CmdType.DL, name, fmt);
                slave[iterate].start();
                iterate++;
            }

            for (int i = 0;i < count; i++) {
                slave[i].join();
            }
            System.out.println("le fichié a été supprimé");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
	
    public static void HdfsWrite(Format.Type fmt, String localFSSourceFname, 
     int repFactor) {
        HdfsConfig cfg = new HdfsConfig();
        Path p = Paths.get(localFSSourceFname);
        String fileName = p.getFileName().toString();
        long startTime = System.currentTimeMillis();
        try {
            NameProvider provider = (NameProvider)Naming.lookup("//"+cfg.getProviderAddress()+":"+cfg.getProviderPort()+"/NameProvider");
            List<HdfsServer> servers = provider.getServers();

            Map<String,HdfsServer> cd = new HashMap<String,HdfsServer>();

            Format hdfsFormat = null;

            switch(fmt) {
                case KV:
                    hdfsFormat = new KVFormat(localFSSourceFname);
                    break;
                case LINE:
                    hdfsFormat = new LineFormat(localFSSourceFname);
                    break;
            }
            hdfsFormat.open(Format.OpenMode.R);
            int count = servers.size();
            ClientSlave slaves[] = new ClientSlave[count];
            for (int i = 0; i < count; i++) {
                short port = (short) servers.get(i).getPort();
                String address = servers.get(i).getAddress();
                slaves[i] = new ClientSlave(address, port, CmdType.WR, fileName+".hdfschunk_"+i, fmt);
                slaves[i].start();
            }

            KV kv;
            int i;
            for (i = 0, kv = hdfsFormat.read(); kv != null; i++, kv = hdfsFormat.read()) {
                int serverId = (i / HdfsServer.DEFAULT_BLOCK_SIZE) % count;
                slaves[serverId].put(new KV(kv.k, kv.v));
            }

            for (i = 0; i < count; i++) {
                slaves[i].put(new KV(null, null));
            }
            for (i = 0; i < count; i++) {
                slaves[i].join();
                cd.put(fileName+".hdfschunk_"+i, servers.get(i));
            }

            HdfsFile file = new HdfsFile(fileName, cd, count, fmt);
            provider.addFile(file);

        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Temps écoulé: " + (System.currentTimeMillis()-startTime)/1000 + " secondes");
    }
    
    public static void HdfsRead(String hdfsFname, String localFSDestFname) {
        HdfsConfig cfg = new HdfsConfig();
        long startTime = System.currentTimeMillis();
        try {
            NameProvider provider = (NameProvider)Naming.lookup("//"+cfg.getProviderAddress()+":"+cfg.getProviderPort()+"/NameProvider");

            HdfsFile hdfsfile = provider.getFile(hdfsFname);
            Format.Type fmt = hdfsfile.getType();
            Map<String,HdfsServer> servers = hdfsfile.getDistribution();
            
            Format hdfsFormat = null;

            if (fmt == Format.Type.KV) {
                    hdfsFormat = new KVFormat(localFSDestFname);
            } else {
                
                    hdfsFormat = new LineFormat(localFSDestFname);
            }
            hdfsFormat.open(Format.OpenMode.W);

            int count = servers.size();
            String address;
            ClientSlave slave[] = new ClientSlave[count];
            int iterate = 0;
            for (String key : servers.keySet()) {
                short port = (short) servers.get(key).getPort();
                address = servers.get(key).getAddress();
                String name = key;
                slave[iterate] = new ClientSlave(address, port, CmdType.RD, name, fmt);
                slave[iterate].start();
                iterate++;
            }

            boolean finished[] = new boolean[count];
            int finishCount = 0;
            for (int i = 0; i < count ; i++) {
                finished[i] = false;
            }

            for (long i = 0; ; i++) {
                int serverId = (int)((i / HdfsServer.DEFAULT_BLOCK_SIZE) % count);
                if (!finished[serverId]) {
                    KV kv = slave[serverId].take();
                    if (kv.k == null) {
                        finishCount++;
                        finished[serverId] = true;
                    } else {
                        hdfsFormat.write(kv);
                    }
                }

                if (finishCount == count) {
                    break;
                }
            }

            for (int i = 0; i < count; i++) {
                slave[i].join();
            }

            hdfsFormat.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("Temps écoulé: " + (System.currentTimeMillis()-startTime)/1000 + " secondes");
    }

	
    public static void main(String[] args) {
        // java HdfsClient <read|write> <line|kv> <file>

        try {
            if (args.length<2) {usage(); return;}

            switch (args[0]) {
              case "read": HdfsRead(args[1],args[2]); break;
              case "delete": HdfsDelete(args[1]); break;
              case "write": 
                Format.Type fmt;
                if (args.length<3) {usage(); return;}
                if (args[1].equals("line")) fmt = Format.Type.LINE;
                else if(args[1].equals("kv")) fmt = Format.Type.KV;
                else {usage(); return;}
                HdfsWrite(fmt,args[2],1);
            }	
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

}
