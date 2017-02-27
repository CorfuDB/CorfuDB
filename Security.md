# Security in CorfuDB

## TLS support in CorfuDB

### Enabling TLS on the Corfu Infrastructure

TLS can be enabled on the corfu infrastructure by using the ```-e``` option while running ```corfu_server```. ```corfu_server``` provides additional options to specify the key/trust stores, key/trust store passwords, cipher suites and protocols to use.
 
```
Corfu Server, the server for the Corfu Infrastructure.

Usage:
	corfu_server (-l <path>|-m) [-nsQ] [-a <address>] [-t <token>] [-c <size>] [-k seconds] [-d <level>] [-p <seconds>] [-M <address>:<port>] [-e [-u <keystore> -f <keystore_password_file>] [-r <truststore> -w <truststore_password_file>] [-b] [-g -o <username_file> -j <password_file>] [-x <ciphers>] [-z <tls-protocols>]] <port>

Options:
 -l <path>, --log-path=<path>                                                           Set the path to the storage file for the log unit.
 -s, --single                                                                           Deploy a single-node configuration.
                                                                                        The server will be bootstrapped with a simple one-unit layout.
 -a <address>, --address=<address>                                                      IP address to advertise to external clients [default: localhost].
 -m, --memory                                                                           Run the unit in-memory (non-persistent).
                                                                                        Data will be lost when the server exits!
 -c <size>, --max-cache=<size>                                                          The size of the in-memory cache to serve requests from -
                                                                                        If there is no log, then this is the max size of the log unit
                                                                                        evicted entries will be auto-trimmed. [default: 1000000000].
 -t <token>, --initial-token=<token>                                                    The first token the sequencer will issue, or -1 to recover
                                                                                        from the log. [default: -1].
 -p <seconds>, --compact=<seconds>                                                      The rate the log unit should compact entries (find the,
                                                                                        contiguous tail) in seconds [default: 60].
 -d <level>, --log-level=<level>                                                        Set the logging level, valid levels are: 
                                                                                        ERROR,WARN,INFO,DEBUG,TRACE [default: INFO].
 -Q, --quickcheck-test-mode                                                             Run in QuickCheck test mode
 -M <address>:<port>, --management-server=<address>:<port>                              Layout endpoint to seed Management Server
 -n, --no-verify                                                                        Disable checksum computation and verification.
 -e, --enable-tls                                                                       Enable TLS.
 -u <keystore>, --keystore=<keystore>                                                   Path to the key store.
 -f <keystore_password_file>, --keystore-password-file=<keystore_password_file>         Path to the file containing the key store password.
 -b, --enable-tls-mutual-auth                                                           Enable TLS mutual authentication.
 -r <truststore>, --truststore=<truststore>                                             Path to the trust store.
 -w <truststore_password_file>, --truststore-password-file=<truststore_password_file>   Path to the file containing the trust store password.
 -g, --enable-sasl-plain-text-auth                                                      Enable SASL Plain Text Authentication.
 -o <username_file>, --sasl-plain-text-username-file=<username_file>                    Path to the file containing the username for SASL Plain Text Authentication.
 -j <password_file>, --sasl-plain-text-password-file=<password_file>                    Path to the file containing the password for SASL Plain Text Authentication.
 -x <ciphers>, --tls-ciphers=<ciphers>                                                  Comma separated list of TLS ciphers to use.
                                                                                        [default: TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256].
 -z <tls-protocols>, --tls-protocols=<tls-protocols>                                    Comma separated list of TLS protocols to use.
                                                                                        [default: TLSv1.1,TLSv1.2].
 -h, --help                                                                             Show this screen
 --version                                                                              Show version

```

### Enabling TLS on the Corfu Runtime

```java
    CorfuRuntime rt = new CorfuRuntime("localhost:9000");
    rt.enableTls("keystore.jks", "keystore_password.txt", "truststore.jks", "truststore_password.txt");
    rt.connect();
```

### Corfu TLS Basics

Corfu supports asymmetric key encryption. Asymmetric cryptography uses private/public key pairs to encrypt and decrypt data.

Please refer to the following resources for additional information:

 http://docs.oracle.com/javase/8/docs/technotes/guides/security/crypto/CryptoSpec.html
 
 http://docs.oracle.com/javase/8/docs/technotes/guides/security/jsse/JSSERefGuide.html
 
In order to properly configure TLS on CorfuDB you need to specify the location of two key stores to the corfu infrastructure and the runtime.
 * **_Key Store_**: The key store contains the private key and certificate used by the infrastructure or the runtime.
 * **_Trust Store_**: The trust store contains the certificates of trusted entities. Trust stores don't contain private keys.
 
The key stores are password protected. CorfuDB requires that the passwords be stored in a file, in plain-text. 
You can point the infrastructure and the runtime to the password files.

You can use the ```keytool``` utility to create and modify the key stores.

### Creating a key store

#### Using Self-Signed Certificates

You can use the ```keytool -genkeypair``` command to create a private/public key pair for the TLS endpoint.
In addition to generating a key pair, this command also creates a self-signed certificate.

For example, the following command generates a private/public key pair, creates a self-signed certificate and stores them in a key store protected by the given password.

```bash
$ keytool -genkeypair -keystore s1.jks -keyalg RSA -keysize 2048 -alias s1 -storepass test123
```

The given key store password is then saved in a file. The permissions to this plain-text password file is then set such that it can only be read by members of the appropriate group.

#### Using Certificates Signed by a Certificate Authority

To use a certificate signed by a CA, do the following additional steps.

* Create a certificate sign request using the ```keytool -certreq``` command.
```bash
$ keytool -certreq -keystore s1.jks -storepass test123 -alias s1 -file s1.csr
```
* Submit the CSR to the CA and get a signed certificate back.
* Import the signed certificate into the key store.
```bash
keytool -importcert -keystore s1.jks -storepass test123 -alias s1 -trustcacerts -file s1.crt
```

### Creating a trust store

You can use the ```keytool -importcert``` / ```keytool -exportcert``` commands to import/export certificates to/from key stores.

When using self-signed certificates, the trust store should have the certificates of all other endpoints.
When using certificates signed by a certificate authority, only the certificate of the certificate authority is sufficient.

### TLS Mutual Authentication

TLS mutual authentication can be enabled on ```corfu_server``` using the ```-b``` option. By default, mutual authentication is disabled, i.e: the clients/runtimes are not authenticated by corfu_server via TLS.

## SASL Plain-Text Authentication in CorfuDB

CorfuDB supports SASL Plain Text authentication of clients. This feature can be enabled on the CorfuDB infrastructure as follows:

* Create a JAAS config file for CorfuDB. A sample config file, ```corfudb_jaas.config``` can be found in the ```test/src/test/resources/security``` directory.
```bash
CorfuDB {
    org.corfudb.security.sasl.plaintext.PlainTextLoginModule required
    debug="true"
    corfudb_user_user1="user1_password"
    corfudb_user_user2="user2_password";
};
```
CorfuDB expects the application name to be ```CorfuDB```.
The usernames and passwords required to authenticate clients can be provided to the PlainTextLoginModule via the ModuleOptions as shown above.
ModuleOption with a key prefixed by ```corfudb_user_``` defines a username and the associated value is the password.
In the example config above, PlainTextLoginModule can authenticate two users:
```
Username: user1
Password: user1_password

and

Username: user2
Password: user2_password
```

In this way, the JAAS config file is used to specify usernames and associated passwords.
For more details on JAAS and the config file, please refer to the following resources:

http://docs.oracle.com/javase/8/docs/technotes/guides/security/jaas/JAASRefGuide.html

http://docs.oracle.com/javase/8/docs/api/javax/security/auth/login/Configuration.html


* Set the ```java.security.auth.login.config``` JVM parameter to point to the JAAS config file created in step 1.
You can do this by setting the ```SERVER_JVMFLAGS``` environment variable.

```bash
$ export SERVER_JVMFLAGS="-Djava.security.auth.login.config=test/src/test/resources/security/corfudb_jaas.config"
```

* Run ```corfu_server``` with the ```-g``` option. Also provide the username and password that the runtimes can use to connect to the server via the ```-o``` and ```-j``` options.

For example:
```bash
$ bin/corfu_server -m -s -e -u test/src/test/resources/security/s1.jks -f test/src/test/resources/security/storepass -r test/src/test/resources/security/s1.jks -w test/src/test/resources/security/storepass -g -o test/src/test/resources/security/username1 -j test/src/test/resources/security/userpass1 9000
```

Please note: TLS is a prerequisite for SASL Plain Text Authentication.

* Enable SASL on the runtimes as follows:
Write the username to a file, say ```username.txt```, and the associated password to another file, say ```userpassword.txt```.
Call the ```enableSaslPlainText()``` method of CorfuRuntime before calling the ```connect()```.

For example:
```java
    CorfuRuntime rt = new CorfuRuntime("localhost:9000");
    rt.enableTls("keystore.jks", "keystore_password.txt", "truststore.jks", "truststore_password.txt");
    rt.enableSaslPlainText("username.txt", "userpassword.txt");
    rt.connect();
```

## Complete Examples

#### Self-Signed Certificates
```bash
# Generate a key pair for the corfu infrastructure:
$ keytool -genkeypair -keystore s1.jks -keyalg RSA -keysize 2048 -alias s1 -storepass password1

# Store the password in a plain text file:
$ cat > s1pass.txt
password1
^C
$

# Export the corfu infrastructure cert to a file
$ keytool -exportcert -keystore s1.jks -storepass password1 -alias s1 -rfc -file s1.cert


# Generate a key pair for the corfu runtime:
$ keytool -genkeypair -keystore r1.jks -keyalg RSA -keysize 2048 -alias r1 -storepass password2


# Store the password in a plain text file:
$ cat > r1pass.txt
password2
^C
$

# Export the corfu runtime cert to a file
$ keytool -exportcert -keystore r1.jks -storepass password2 -alias r1 -rfc -file r1.cert


# Import the corfu infrastructure and runtime certs into a trust store
$ keytool -importcert -keystore trust.jks -storepass password3 -alias s1 -file s1.cert
$ keytool -importcert -keystore trust.jks -storepass password3 -alias r1 -file r1.cert
# Store the password in a plain text file:
$ cat > tpass.txt
password3
^C
$
```

Set up the JAAS config file.

```bash
$ cat > corfudb_jaas.config
CorfuDB {
    org.corfudb.security.sasl.plaintext.PlainTextLoginModule required
    debug="true"
    corfudb_user_admin="@dm1np@ssw0rd"
    corfudb_user_user="userp@ssw0rd";
};
^C
```

Store the username and password in separate files
```bash
$ cat > username.txt
admin
^C
$ cat > userpassword.txt
@dm1np@ssw0rd
^C
$
```

Set the ```SERVER_JVMFLAGS``` environment variable
```bash
$ export SERVER_JVMFLAGS="-Djava.security.auth.login.config=corfudb_jaas.config"
```

Run ```corfu_server```
```bash
$ bin/corfu_server -m -e -u s1.jks -f s1pass.txt -r trust.jks -w tpass.txt -g -o username.txt -j userpassword.txt --single 9000
```

Runtime:

Store the username and password in separate files
```bash
$ cat > r1username.txt
user
^C
$ cat > r1userpassword.txt
userp@ssw0rd
^C
$
```

```java
    CorfuRuntime rt = new CorfuRuntime("localhost:9000");
    rt.enableTls("r1.jks", "r1pass.txt", "trust.jks", "tpass.txt");
    rt.enableSaslPlainText("r1username.txt", "r1userpassword.txt");
    rt.connect();
```


#### CA Signed Certificates

```bash
# Generate a key pair for the corfu infrastructure:
$ keytool -genkeypair -keystore s1.jks -keyalg RSA -keysize 2048 -alias s1 -storepass password1

# Store the password in a plain text file:
$ cat > s1pass.txt
password1
^C
$

# Generate a CSR
$ keytool -certreq -keystore s1.jks -storepass password1 -alias s1 -file s1.csr

# Submit the CSR to the CA for signing. Get the signed cert back in s1.cert
# Import the signed certificate into the keystore
$ keytool -importcert -keystore s1.jks -storepass password1 -alias s1 -file s1.cert


# Generate a key pair for the corfu runtime:
$ keytool -genkeypair -keystore r1.jks -keyalg RSA -keysize 2048 -alias r1 -storepass password2

# Store the password in a plain text file:
$ cat > r1pass.txt
password2
^C
$
# Generate a CSR
$ keytool -certreq -keystore r1.jks -storepass password2 -alias r1 -file r1.csr

# Submit the CSR to the CA for signing. Get the signed cert back in r1.cert
# Import the signed certificate into the keystore
$ keytool -importcert -keystore r1.jks -storepass password2 -alias r1 -file r1.cert


# Import the CA cert into a trust store
$ keytool -importcert -keystore trust.jks -storepass password3 -alias ca -file cacert.cert
# Store the password in a plain text file:
$ cat > tpass.txt
password3
^C
$
```

Run corfu_server
```bash
$ bin/corfu_server -m -e -u s1.jks -f s1pass.txt -r trust.jks -w tpass.txt --single 9000
```

Runtime:

```java
    CorfuRuntime rt = new CorfuRuntime("localhost:9000");
    rt.enableTls("r1.jks", "r1pass.txt", "trust.jks", "tpass.txt");
    rt.connect();
```
