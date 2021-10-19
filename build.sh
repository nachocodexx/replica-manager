readonly TAG=$1
readonly DOCKER_IMAGE=nachocode/load-balancer
sbt assembly && docker build -t "$DOCKER_IMAGE:$TAG" .
