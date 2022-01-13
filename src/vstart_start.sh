#!/bin/bash

cd ~/ceph/build

make -j20

# NUM_MDS=3
# VSTART_DEST=~/ceph/build MDS=${NUM_MDS} ../src/vstart.sh -l -n -d --multimds ${NUM_MDS}
VSTART_DEST=~/ceph/build ../src/vstart.sh -l -n -d

# ./bin/ceph --admin-daemon ./out/mds.a.asok config set mds_dmclock_enable true
# ./bin/ceph --admin-daemon ./out/mds.a.asok config set mds_dmclock_reservation 30
# ./bin/ceph --admin-daemon ./out/mds.a.asok config set mds_dmclock_weight 50
# ./bin/ceph --admin-daemon ./out/mds.a.asok config set mds_dmclock_limit 100

# ./bin/ceph --admin-daemon ./out/mds.b.asok config set mds_dmclock_enable true
# ./bin/ceph --admin-daemon ./out/mds.b.asok config set mds_dmclock_reservation 30
# ./bin/ceph --admin-daemon ./out/mds.b.asok config set mds_dmclock_weight 50
# ./bin/ceph --admin-daemon ./out/mds.b.asok config set mds_dmclock_limit 100

# ./bin/ceph --admin-daemon ./out/mds.c.asok config set mds_dmclock_enable true
# ./bin/ceph --admin-daemon ./out/mds.c.asok config set mds_dmclock_reservation 30
# ./bin/ceph --admin-daemon ./out/mds.c.asok config set mds_dmclock_weight 50
# ./bin/ceph --admin-daemon ./out/mds.c.asok config set mds_dmclock_limit 100

echo "./bin/ceph fs subvolume create a subvolume"
./bin/ceph fs subvolume create a subvolume --size $((100*1024*1024*1024))
SUBVOL=`./bin/ceph fs subvolume getpath a subvolume`
echo "SUBVOL: $SUBVOL"
##
##sudo ./bin/ceph-fuse -n client.admin -c ceph.conf -k keyring --client_mountpoint=/ ./mnt/
##sudo setfattr -n ceph.dmclock.mds_reservation -v 30 ./mnt/volumes/_nogroup/subvolume/
##sudo setfattr -n ceph.dmclock.mds_weight -v 50 ./mnt/volumes/_nogroup/subvolume/
##sudo setfattr -n ceph.dmclock.mds_limit -v 100 ./mnt/volumes/_nogroup/subvolume/
##
#sudo ./bin/ceph-fuse -n client.admin -c ceph.conf -k keyring --client_mountpoint=$SUBVOL ./mnt_user/
##
##./bin/ceph --admin-daemon ./out/mds.a.asok dump qos
##./bin/ceph --admin-daemon ./out/mds.b.asok dump qos
##./bin/ceph --admin-daemon ./out/mds.c.asok dump qos


# Kernel mount
monip=$(grep 'mon host' ceph.conf | grep -o '[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}' | uniq | head -n 1)
port=`grep 'mon host' ceph.conf | awk -F":" '{print $3}' | awk -F"," '{print $1}'`
key=`grep -A 1 admin keyring | grep key | awk -F "=" '{print $2}' | sed -e 's/^[[:space:]]*//'`

# echo "sudo mount -t ceph ${monip}:${port},${monip}:$((port+1)):$SUBVOL /mn/t/ceph_ssd -o name=admin,secret=${key}=="
# sudo mount -t ceph ${monip}:${port},${monip}:$((port+1)):$SUBVOL /mnt/ceph_ssd/ -o name=admin,secret=${key}==
echo "sudo mount -t ceph ${monip}:${port},${monip}:$((port+1)):$SUBVOL ./new_mnt/ -o name=admin,secret=${key}=="
sudo mount -t ceph ${monip}:${port},${monip}:$((port+1)):$SUBVOL ./new_mnt/ -o name=admin,secret=${key}==
