#!/bin/bash

set -ex

WORKDIR=$(pwd)

# update rkdev
cd ${WORKDIR}/rkdev
./kdev-build.sh
cp -a rkdev_arm64 ${WORKDIR}/rootfs/
sync

# build kernel
cd ${WORKDIR}/
#make ARCH=arm64 \
#  CROSS_COMPILE=aarch64-linux-gnu- \
#  KBUILD_BUILD_USER="builder" \
#  KBUILD_BUILD_HOST="kdevbuilder" \
#  LOCALVERSION=-kdev \
#  mrproper

# build kernel Image
make ARCH=arm64 \
  CROSS_COMPILE=aarch64-linux-gnu- \
  KBUILD_BUILD_USER="builder" \
  KBUILD_BUILD_HOST="kdevbuilder" \
  LOCALVERSION=-kdev \
  aiot_3588ied_defconfig

# check kver
KVER=$(make LOCALVERSION=-kdev kernelrelease)
KVER="${KVER/kdev*/kdev}"
if [[ "$KVER" != *kdev ]]; then
  echo "ERROR: KVER does not end with 'kdev'"
  exit 1
fi
echo "KVER: ${KVER}"

make ARCH=arm64 \
  CROSS_COMPILE=aarch64-linux-gnu- \
  KBUILD_BUILD_USER="builder" \
  KBUILD_BUILD_HOST="kdevbuilder" \
  LOCALVERSION=-kdev \
  rockchip/rk3588-aiot-3588ied.dtb \
  -j$(nproc)

make ARCH=arm64 \
  CROSS_COMPILE=aarch64-linux-gnu- \
  KBUILD_BUILD_USER="builder" \
  KBUILD_BUILD_HOST="kdevbuilder" \
  LOCALVERSION=-kdev \
  KCFLAGS="-Wno-unused-function" \
  -j$(nproc)

make ARCH=arm64 \
  CROSS_COMPILE=aarch64-linux-gnu- \
  KBUILD_BUILD_USER="builder" \
  KBUILD_BUILD_HOST="kdevbuilder" \
  LOCALVERSION=-kdev \
  KCFLAGS="-Wno-unused-function" \
  modules -j$(nproc)

make ARCH=arm64 \
  CROSS_COMPILE=aarch64-linux-gnu- \
  KBUILD_BUILD_USER="builder" \
  KBUILD_BUILD_HOST="kdevbuilder" \
  LOCALVERSION=-kdev \
  INSTALL_MOD_PATH=$(pwd)/kos \
  modules_install

# update rootfs modules
rm -rf rootfs/var/cache/apk/*
rm -rf rootfs/lib/modules/*
rm -f rootfs/root/.ash_history
rm -f rootfs/var/log/apk.log
rm -rf rootfs/lib/modules/*

du -sh rootfs

# rebuild kernel archive rootfs
make ARCH=arm64 \
  CROSS_COMPILE=aarch64-linux-gnu- \
  KBUILD_BUILD_USER="builder" \
  KBUILD_BUILD_HOST="kdevbuilder" \
  LOCALVERSION=-kdev \
  KCFLAGS="-Wno-unused-function" \
  -j$(nproc)

cat include/config/kernel.release
ls -alh arch/arm64/boot/Image

rm -rf ${WORKDIR}/output
rm -rf recovery
mkdir -p recovery

# archive kernel image
xz --format=lzma -k -c arch/arm64/boot/Image >recovery/kernel.lzma
ls -al recovery/kernel.lzma

# generate recovery
cd ${WORKDIR}/recovery
dd if=/dev/zero of=recovery.img bs=1M count=63
mkimage -A arm64 -O linux -T kernel -C lzma \
  -a 0x40080000 -e 0x40080000 \
  -n "Recovery Kernel" \
  -d kernel.lzma kernel-uImage.lzma
mkimage -l kernel-uImage.lzma

mkfs.ext4 \
  -L recovery \
  recovery.img

cleanup() { mountpoint -q /mnt && umount /mnt || true; }
trap cleanup EXIT

mount recovery.img /mnt
rmdir /mnt/lost+found || :

cp -a ${WORKDIR}/arch/arm64/boot/dts/rockchip/rk3588-aiot-3588ied.dtb \
  /mnt/rk3588-aiot-3588ied.dtb
cp -a kernel-uImage.lzma /mnt
tee /mnt/recovery.conf <<'EOF'
label RK3588 Linux recovery
    kernel kernel-uImage.lzma
    fdt rk3588-aiot-3588ied.dtb
    append console=ttyS2,1500000n8 earlycon=uart8250,mmio32,0xfeb50000 rootwait rw
EOF
umount /mnt
sync
mkdir -p ${WORKDIR}/output/
cp -a recovery.img ${WORKDIR}/output/recovery.img
sync

ls -alh ${WORKDIR}/output/*.img
sha256sum ${WORKDIR}/output/*.img
ls -alh ${WORKDIR}/recovery/kernel.lzma
sha256sum ${WORKDIR}/recovery/kernel.lzma

echo "All done!All ok!"
