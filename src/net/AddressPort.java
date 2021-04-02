package net;

import java.lang.IllegalArgumentException;

public class AddressPort {
    public static final short DEFAULT_PORT = 4000;
    private short port;
    private String addr;

    public AddressPort() {
        this.port = DEFAULT_PORT;
        this.addr = getAddressFromName("localhost");
    }

    public AddressPort(String addr) {
        try {
            this.port = getPortFromName(addr);
        } catch (IllegalArgumentException e) {
            this.port = DEFAULT_PORT;
        }
        this.addr = getAddressFromName(addr);
    }

    public short getPort() {
        return this.port;
    }

    public String getAddress() {
        return this.addr;
    }

    public static short getPortFromName(String addr) throws IllegalArgumentException {
        String lc[] = addr.split(":");
        if (lc.length > 2) {
            throw new IllegalArgumentException("Invalid address or missing port number");
        } else if (lc.length < 2) {
            return -1;
        }
        return new Short(lc[1]);
    }

    public static String getAddressFromName(String addr) {
        String lc[] = addr.split(":");
        return lc[0];
    }
}