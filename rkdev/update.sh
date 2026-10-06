#!/bin/bash

set -x

IP=${1:-192.168.33.38}

./kdev-build.sh
sshpass -p root ssh root@"$IP" pkill rkdev_arm64
sshpass -p root scp rkdev_arm64 root@"$IP":/root/
