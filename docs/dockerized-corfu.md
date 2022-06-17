## Dockerized corfu

CorfuDb is available as a Docker image. The image uses `alpine` as a base image.
CorfuDb repository [is available on docker hub](https://hub.docker.com/repository/docker/corfudb/corfu-server)

#### Building corfu image
`.ci/infrastructure-docker-build.sh docker`
 
#### Deploying corfu image
`./mvnw -pl infrastructure -DskipTests clean deploy`

#### Pulling the image (particular version)
`docker pull corfudb/corfu-server:${CORFU_VERSION}`
 
#### Running corfu server
`docker run -ti corfudb/corfu-server:${CORFU_VERSION}`
