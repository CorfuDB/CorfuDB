## Dockerized corfu

Corfu is available as Docker image. The image uses `alpine` as the base image.
Corfu repository is [here](https://cloud.docker.com/u/corfudb/repository/docker/corfudb/corfu-server)

#### Building corfu image
`./mvnw -pl infrastructure -DskipTests -Pdocker clean package`
 
#### Deploying corfu image
`./mvnw -pl infrastructure -DskipTests -Pdocker clean deploy`

#### Pulling the image (particular version)
`docker pull corfudb/corfu-server:0.3.0-SNAPSHOT`
 
#### Running corfu server
`docker run -ti corfudb/corfu-server:0.3.0-SNAPSHOT`

P.S.
The new Docker image does not support any cmdlets.
If required the cmdlets can be added to the new Docker image in the future.