readonly TAG=$1
readonly DOCKER_IMAGE=nachocode/replica-manager
sbt assembly && docker build -t "$DOCKER_IMAGE:$TAG" .
