package com.tencent;

public class HostToPort {
	private String hostip;
	private String hostport;

	public String getHostip() {
		return hostip;
	}

	public void setHostip(String hostip) {
		this.hostip = hostip;
	}

	public String getHostport() {
		return hostport;
	}

	public void setHostport(String hostport) {
		this.hostport = hostport;
	}

	public String toString() {
		return hostip + ":" + hostport;
	}

}
