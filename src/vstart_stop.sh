#!/bin/bash

cd ~/ceph/build
#sudo fusermount -u /home/daeyang/ceph/build/mnt_user/
echo "umount kernel..."
sudo umount /home/hong/ceph/build/mnt_user/
# sudo umount /home/hong/mnt/ceph_ssd_0/
echo "Complete...!"
../src/stop.sh
