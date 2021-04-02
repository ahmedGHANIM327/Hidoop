package hdfs;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

import formats.Format.Type;
import hdfs.ClientSlave.CmdType;

public class HdfsIO {
    public static final int MAX_STRING_LENGTH = 1024;
    public static final long TIMEOUT_MILLIS = 10000;
    private byte[] intBuf;
    private byte[] stringBuf;

    static public byte[] toBytes(int v) {
        byte[] b = new byte[4];
        b[0] = (byte)(v & 0xFF);
        b[1] = (byte)((v >> 8) & 0xFF);
        b[2] = (byte)((v >> 16) & 0xFF);
        b[3] = (byte)((v >> 24) & 0xFF);
        return b;
    }

    static public byte[] toBytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    static public int intFromBytes(byte[] b) {
        assert(b.length == 4);
        return  (b[0] & 0xFF) +
               ((b[1] & 0xFF) << 8) +
               ((b[2] & 0xFF) << 16) +
               ((b[3] & 0xFF) << 24);
    }

    public HdfsIO() {
        intBuf = new byte[4];
        stringBuf = new byte[HdfsServer.MAX_STRING_LENGTH];
    }

    private void read(InputStream is, byte buf[], int size) throws IOException, TimeoutException {
        long lastTime = System.currentTimeMillis();
        if (is.available() < size) {
            for (int i = 0; i < size; i++) {
                int v;
                while ((v = is.read()) == -1) {
                    if (System.currentTimeMillis() - lastTime > TIMEOUT_MILLIS) {
                        throw new TimeoutException("read: timeout");
                    }
                }
                buf[i] = (byte)v;
            }
        } else {
            is.read(buf, 0, size);
        }
    }

    private void read(InputStream is, byte buf[]) throws IOException, TimeoutException {
        read(is, buf, buf.length);
    }

    // Readers
    public int readInt(InputStream is) throws IOException, TimeoutException {
        read(is, intBuf);
        return HdfsIO.intFromBytes(intBuf);
    }

    public String readString(InputStream is) throws IOException, TimeoutException {
        int length = readInt(is);
        if (length > HdfsServer.MAX_STRING_LENGTH) {
            System.out.printf("ERREUR: chaîne caractères trop longue: %d > %d\n", length, HdfsServer.MAX_STRING_LENGTH);
            throw new IOException();
        }
        if (length == 0) {
            return "";
        }
        read(is, stringBuf, length);
        return new String(stringBuf, 0, length, StandardCharsets.UTF_8);
    }

    public CmdType readCmdType(InputStream is) throws IOException, TimeoutException {
        int index = readInt(is);
        return CmdType.values()[index];
    }

    public Type readFmtType(InputStream is) throws IOException, TimeoutException {
        int index = readInt(is);
        return Type.values()[index];
    }

    // Writers
    public void writeInt(OutputStream os, int v) throws IOException {
        os.write(toBytes(v));
    }

    public void writeString(OutputStream os, String s) throws IOException {
        byte sBytes[] = toBytes(s);
        writeInt(os, sBytes.length);
        os.write(sBytes);
    }

    public void writeEnum(OutputStream os, Enum<?> e) throws IOException {
        os.write(toBytes(e.ordinal()));
    }
}
