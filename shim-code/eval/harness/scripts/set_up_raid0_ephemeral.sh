umount /mnt
yes | mdadm --create /dev/md0 --level=0 -c256 --raid-devices=2 /dev/xvdb /dev/xvdc
echo 'DEVICE /dev/xvdb /dev/xvdc' > /etc/mdadm.conf

mdadm --detail --scan >> /etc/mdadm.conf

blockdev --setra 65536 /dev/md0
mkfs.xfs -f /dev/md0
mkdir -p /mnt/md0 && mount -t xfs -o noatime /dev/md0 /mnt/md0
cd /mnt/md0

perl -ne 'print if $_ !~ /mnt/' /etc/fstab > /etc/fstab.2
echo '#/dev/md0  /mnt  xfs    defaults 0 0' >> /etc/fstab.2
mv /etc/fstab.2 /etc/fstab