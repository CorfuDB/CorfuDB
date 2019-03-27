## Dockerized corfu

Corfu is available as Docker image. The image uses `alpine` as the base image.
Corfu repository is [here](https://cloud.docker.com/u/corfudb/repository/docker/corfudb/corfu-server)

#### Building corfu image
`./mvnw -pl infrastructure -DskipTests -Pdocker clean package`
 
#### Deploing corru image
`./mvnw -pl infrastructure -DskipTests -Pdocker clean deploy`

#### Pulling the image (particular version)
`docker pull corfudb/corfu-server:0.2.3-SNAPSHOT`
 
#### Running corfu server
`docker run -ti corfudb/corfu-server:0.2.3-SNAPSHOT` 