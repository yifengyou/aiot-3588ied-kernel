#!/bin/bash

set -ex

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
rm -f  rootfs/root/.ash_history
cp -a kos/lib/modules/* rootfs/lib/modules/
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

rm -rf recovery
mkdir -p recovery

# kernel image
xz --format=lzma -k -c arch/arm64/boot/Image > recovery/kernel.lzma
ls -alh recovery/kernel.lzma
# dtb
cp -a arch/arm64/boot/dts/rockchip/rk3588-aiot-3588ied.dtb recovery/
cd recovery

dd if=/dev/zero of=recovery.img bs=1M count=64
mkimage -A arm64 -O linux -T kernel -C lzma \
    -a 0x40080000 -e 0x40080000 \
    -n "Recovery Kernel" \
    -d kernel.lzma kernel-uImage.lzma
mkfs.ext4 \
    -O '^metadata_csum,^has_journal,^resize_inode' \
    -N 16 -m 0 \
    -L recovery \
    recovery.img

mount recovery.img /mnt
df -h
cp -a rk3588-aiot-3588ied.dtb /mnt/
cp -a kernel-uImage.lzma /mnt
df -h

tee /mnt/recovery.conf << 'EOF'
label RK3588 Linux recovery
    kernel kernel-uImage.lzma
    fdt rk3588-aiot-3588ied.dtb
    append console=ttyS2,1500000n8 earlycon=uart8250,mmio32,0xfeb50000 rootwait rw iommu.passthrough=1
EOF

ls -alh /mnt
umount /mnt

ls -alh recovery.img
sha256sum recovery.img

echo "All done!All ok!"
