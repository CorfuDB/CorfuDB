package com.microsoft.corfu;

// This is a Corfu endpoint
//
public class CorfuNode {
	String hostname;
	int port;
	Object info;
	
	public CorfuNode(String fullname)
	{
		hostname = fullname.substring(0, fullname.indexOf(":"));
		port = Integer.parseInt(fullname.substring(fullname.indexOf(":")+1));
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

	public void setInfo(Object val) { info = val; }
	public Object getInfo() { return info; }
}
