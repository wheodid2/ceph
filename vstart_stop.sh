#!/bin/bash

cd ~/ceph/build
sudo fusermount -u /home/daeyang/ceph/build/mnt_user/
../src/stop.sh
