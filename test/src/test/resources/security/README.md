### Commands used for generating RSA and ECDSA keystores are as below
```bash
keytool -genkeypair -keystore server_rsa.jks -keyalg RSA -keysize 2048 -alias server_rsa -storepass test123 -keypass test123 -dname "cn=Corfu, ou=NSBU, o=VMware, l=Palo Alto, s=CA c=US" -validity 2000000 
keytool -genkeypair -keystore server_rsa_2.jks -keyalg RSA -keysize 2048 -alias server_rsa_2 -storepass test123 -keypass test123 -dname "cn=Corfu, ou=NSBU, o=VMware, l=Palo Alto, s=CA c=US" -validity 2000000 
keytool -genkeypair -keystore runtime_rsa.jks -keyalg RSA -keysize 2048 -alias runtime_rsa -storepass test123 -keypass test123 -dname "cn=Corfu, ou=NSBU, o=VMware, l=Palo Alto, s=CA c=US" -validity 2000000 
keytool -genkeypair -keystore runtime_rsa_2.jks -keyalg RSA -keysize 2048 -alias runtime_rsa_2 -storepass test123 -keypass test123 -dname "cn=Corfu, ou=NSBU, o=VMware, l=Palo Alto, s=CA c=US" -validity 2000000 

keytool -genkeypair -groupname secp384r1 -sigalg SHA384withECDSA -keyalg EC -dname "cn=Corfu, ou=NSBU, o=VMware, l=Palo Alto, s=CA c=US" -alias server_ecdsa -keypass test123 -keystore server_ecdsa.jks -storetype jks -storepass test123 -validity 2000000 
keytool -genkeypair -groupname secp384r1 -sigalg SHA384withECDSA -keyalg EC -dname "cn=Corfu, ou=NSBU, o=VMware, l=Palo Alto, s=CA c=US" -alias server_ecdsa_2 -keypass test123 -keystore server_ecdsa_2.jks -storetype jks -storepass test123 -validity 2000000 
keytool -genkeypair -groupname secp384r1 -sigalg SHA384withECDSA -keyalg EC -dname "cn=Corfu, ou=NSBU, o=VMware, l=Palo Alto, s=CA c=US" -alias runtime_ecdsa -keypass test123 -keystore runtime_ecdsa.jks -storetype jks  -storepass test123 -validity 2000000
keytool -genkeypair -groupname secp384r1 -sigalg SHA384withECDSA -keyalg EC -dname "cn=Corfu, ou=NSBU, o=VMware, l=Palo Alto, s=CA c=US" -alias runtime_ecdsa_2 -keypass test123 -keystore runtime_ecdsa_2.jks -storetype jks  -storepass test123 -validity 2000000

keytool -importkeystore -destkeystore server_rsa_rsa.jks -srcstoretype jks -deststoretype jks -srcstorepass test123 -deststorepass test123 -srckeystore server_rsa.jks
keytool -importkeystore -destkeystore server_rsa_rsa.jks -srcstoretype jks -deststoretype jks -srcstorepass test123 -deststorepass test123 -srckeystore server_rsa_2.jks
keytool -importkeystore -destkeystore runtime_rsa_rsa.jks -srcstoretype jks -deststoretype jks -srcstorepass test123 -deststorepass test123 -srckeystore runtime_rsa.jks
keytool -importkeystore -destkeystore runtime_rsa_rsa.jks -srcstoretype jks -deststoretype jks -srcstorepass test123 -deststorepass test123 -srckeystore runtime_rsa_2.jks

keytool -importkeystore -destkeystore server_ecdsa_ecdsa.jks -srcstoretype jks -deststoretype jks -srcstorepass test123 -deststorepass test123 -srckeystore server_ecdsa.jks
keytool -importkeystore -destkeystore server_ecdsa_ecdsa.jks -srcstoretype jks -deststoretype jks -srcstorepass test123 -deststorepass test123 -srckeystore server_ecdsa_2.jks
keytool -importkeystore -destkeystore runtime_ecdsa_ecdsa.jks -srcstoretype jks -deststoretype jks -srcstorepass test123 -deststorepass test123 -srckeystore runtime_ecdsa.jks
keytool -importkeystore -destkeystore runtime_ecdsa_ecdsa.jks -srcstoretype jks -deststoretype jks -srcstorepass test123 -deststorepass test123 -srckeystore runtime_ecdsa_2.jks

keytool -importkeystore -destkeystore server_rsa_ecdsa.jks -srcstoretype jks -deststoretype jks -srcstorepass test123 -deststorepass test123 -srckeystore server_rsa.jks
keytool -importkeystore -destkeystore server_rsa_ecdsa.jks -srcstoretype jks -deststoretype jks -srcstorepass test123 -deststorepass test123 -srckeystore server_ecdsa.jks
keytool -importkeystore -destkeystore runtime_rsa_ecdsa.jks -srcstoretype jks -deststoretype jks -srcstorepass test123 -deststorepass test123 -srckeystore runtime_rsa.jks
keytool -importkeystore -destkeystore runtime_rsa_ecdsa.jks -srcstoretype jks -deststoretype jks -srcstorepass test123 -deststorepass test123 -srckeystore runtime_ecdsa.jks
```