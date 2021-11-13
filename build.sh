readonly TAG=$1
readonly DOCKER_IMAGE=nachocode/storage-pool
sbt assembly && docker build -t "$DOCKER_IMAGE:$TAG" .
