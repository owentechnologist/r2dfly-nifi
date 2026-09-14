# curl + python3 + redis-cli in one small image, so the scripts in this project never need
# those installed on the host - only Docker/Podman itself. See toolbox-lib.sh.
FROM docker.io/library/alpine:3.20
RUN apk add --no-cache curl python3 redis
