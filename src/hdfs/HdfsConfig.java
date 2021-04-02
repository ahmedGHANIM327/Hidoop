package hdfs;

import org.json.*;
import java.io.*;

public class HdfsConfig {
    public static final String configFile = "config/hdfs.json";
    private String providerAddress;
    private short providerPort;
    private short serverPort;
    public HdfsConfig() {
        this.providerAddress = "localhost";
        this.providerPort = 4000;
        this.serverPort = 4050;
        try {
            InputStream stream = new FileInputStream(configFile);
            JSONTokener t = new JSONTokener(stream);
            JSONObject obj = new JSONObject(t);

            this.providerAddress = obj.getString("providerAddress");
            this.providerPort = (short)obj.getInt("providerPort");
            this.serverPort = (short)obj.getInt("serverPort");
        } catch (FileNotFoundException e) {
            System.out.printf("Fichier %s non trouvé\n", configFile);
            return;
        }
    }

    public String getProviderAddress() {
        return providerAddress;
    }

    public short getProviderPort() {
        return providerPort;
    }

    public short getServerPort() {
        return serverPort;
    }
}
