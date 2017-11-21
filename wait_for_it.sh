#!/bin/bash
# wait-for-it.sh
# Entry-point bash script to wait for previous pipeline finishing their work

set -e

host="$1"
shift
cmd="$@"

# MLQ wait for MLC service to end
while ping -c 1 "$host"; do
  >&2 echo "The previous pipeline has not started or finished yet, wait."
  sleep 10
done

>&2 echo "All previous queued work has been done - now executing command"
exec $cmd