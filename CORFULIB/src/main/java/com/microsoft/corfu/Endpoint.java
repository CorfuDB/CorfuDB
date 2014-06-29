package com.microsoft.corfu;

import java.util.HashMap;

// This is a Corfu endpoint
//
public class Endpoint {
	static private HashMap<String, Endpoint> epmap = new HashMap<String, Endpoint>();

    private String hostname;
	private int port;
    private Object info;

    // constructor is private; use genEndpoint to generate a new endpoint!
	Endpoint(String fullname)
	{
		hostname = fullname.substring(0, fullname.indexOf(":"));
		port = Integer.parseInt(fullname.substring(fullname.indexOf(":")+1));
        info = null;
	}

    public static Endpoint genEndpoint(String fullname) {
        if (epmap.containsKey(fullname))
            return epmap.get(fullname);
        else {
            Endpoint ep = new Endpoint(fullname);
            if (ep.getPort() <0) return null;
            epmap.put(fullname, ep);
            return ep;
        }
    }

	@Override
	public String toString()
	{
		return hostname + ":" + port;
	}

    public String getHostname() {
        return hostname;
    }

    public void setHostname(String hostname) {
        this.hostname = hostname;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public Object getInfo() {
        return info;
    }

    public void setInfo(Object info) {
        this.info = info;
    }

}
