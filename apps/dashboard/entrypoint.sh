#!/bin/sh
set -e

export STREAMFORGE_ALERTS_HOST=${STREAMFORGE_ALERTS_HOST:-streamforge-alerts}
export STREAMFORGE_ALERTS_PORT=${STREAMFORGE_ALERTS_PORT:-8081}
export STREAMFORGE_CORE_HOST=${STREAMFORGE_CORE_HOST:-streamforge-core}
export STREAMFORGE_CORE_PORT=${STREAMFORGE_CORE_PORT:-8080}

envsubst '${STREAMFORGE_ALERTS_HOST} ${STREAMFORGE_ALERTS_PORT} ${STREAMFORGE_CORE_HOST} ${STREAMFORGE_CORE_PORT}' < /etc/nginx/nginx.conf.template > /tmp/nginx.conf

exec nginx -c /tmp/nginx.conf -g "daemon off;"
