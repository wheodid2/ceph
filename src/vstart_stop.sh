#!/bin/bash

cd ~/ceph/build
#sudo fusermount -u /home/daeyang/ceph/build/mnt_user/
echo "umount kernel..."
# sudo umount /mnt/ceph_ssd
sudo umount ./mnt_user
echo "Complete...!"
../src/stop.sh
