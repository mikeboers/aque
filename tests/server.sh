#!/bin/bash

self="$(cd $(dirname "${BASH_SOURCE[0]}"); pwd)"
sandbox="$self/sandbox"

mkdir -p "$sandbox"

redis-server - <<EOF

	pidfile $sandbox/redis.pid

	# Disable saving.
	save ""

EOF
