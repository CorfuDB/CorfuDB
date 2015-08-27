package org.corfudb.runtime.protocols;


import java.util.concurrent.TimeUnit;

public class Connection {

    public final static int DEFAULT_TIMEOUT = (int) TimeUnit.SECONDS.toMillis(60);

    private final String _host;
    private final int _port;
    private final String _proxyHost;
    private final int _proxyPort;
    private final boolean _proxy;
    private final int _timeout;

    public Connection(String connectionStr) {
        int indexOfTimeout = connectionStr.indexOf("#");
        if (indexOfTimeout > 0) {
            _timeout = Integer.parseInt(connectionStr.substring(indexOfTimeout + 1));
            connectionStr = connectionStr.substring(0, indexOfTimeout);
        } else {
            _timeout = DEFAULT_TIMEOUT;
        }
        int index = connectionStr.indexOf(':');
        if (index >= 0) {
            int slashIndex = connectionStr.indexOf('/');
            if (slashIndex > 0) {
                _host = connectionStr.substring(0, index);
                _port = Integer.parseInt(connectionStr.substring(index + 1, slashIndex));
                int indexOfProxyPort = connectionStr.indexOf(':', slashIndex);
                _proxyHost = connectionStr.substring(slashIndex + 1, indexOfProxyPort);
                _proxyPort = Integer.parseInt(connectionStr.substring(indexOfProxyPort + 1));
                _proxy = true;
            } else {
                _host = connectionStr.substring(0, index);
                _port = Integer.parseInt(connectionStr.substring(index + 1));
                _proxyHost = null;
                _proxyPort = -1;
                _proxy = false;
            }
        } else {
            throw new RuntimeException("Connection string of [" + connectionStr
                    + "] does not match 'host1:port' or 'host1:port/proxyhost1:proxyport'");
        }
    }

    public Connection(String host, int port, int timeout) {
        this(host, port, null, -1, timeout);
    }

    public Connection(String host, int port, String proxyHost, int proxyPort) {
        this(host, port, proxyHost, proxyPort, DEFAULT_TIMEOUT);
    }

    public Connection(String host, int port, String proxyHost, int proxyPort, int timeout) {
        _port = port;
        _host = host;
        if (proxyHost == null) {
            _proxyHost = null;
            _proxyPort = -1;
            _proxy = false;
        } else {
            _proxyHost = proxyHost;
            _proxyPort = proxyPort;
            _proxy = true;
        }
        _timeout = timeout;
    }

    public Connection(String host, int port) {
        this(host, port, null, -1);
    }

    public String getHost() {
        return _host;
    }

    public int getPort() {
        return _port;
    }

    public boolean isProxy() {
        return _proxy;
    }

    public int getProxyPort() {
        return _proxyPort;
    }

    public String getProxyHost() {
        return _proxyHost;
    }

    public int getTimeout() {
        return _timeout;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((_host == null) ? 0 : _host.hashCode());
        result = prime * result + _port;
        result = prime * result + (_proxy ? 1231 : 1237);
        result = prime * result + ((_proxyHost == null) ? 0 : _proxyHost.hashCode());
        result = prime * result + _proxyPort;
        result = prime * result + _timeout;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Connection other = (Connection) obj;
        if (_host == null) {
            if (other._host != null)
                return false;
        } else if (!_host.equals(other._host))
            return false;
        if (_port != other._port)
            return false;
        if (_proxy != other._proxy)
            return false;
        if (_proxyHost == null) {
            if (other._proxyHost != null)
                return false;
        } else if (!_proxyHost.equals(other._proxyHost))
            return false;
        if (_proxyPort != other._proxyPort)
            return false;
        if (_timeout != other._timeout)
            return false;
        return true;
    }

    public String getConnectionStr() {
        if (_proxyHost != null) {
            return _host + ":" + _port + "/" + _proxyHost + ":" + _proxyPort + "#" + _timeout;
        }
        return _host + ":" + _port + "#" + _timeout;
    }

    @Override
    public String toString() {
        return getConnectionStr();
    }

}