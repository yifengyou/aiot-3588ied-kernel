#!/bin/bash

set -ex

rm -f arch/arm64/boot/dts/rockchip/rk3588-aiot-3588ied.dtb

make ARCH=arm64 \
  CROSS_COMPILE=aarch64-linux-gnu- \
  KBUILD_BUILD_USER="builder" \
  KBUILD_BUILD_HOST="kdevbuilder" \
  LOCALVERSION=-kdev \
  rockchip/rk3588-aiot-3588ied.dtb \
   -j$(nproc)

ls -alh arch/arm64/boot/dts/rockchip/rk3588-aiot-3588ied.dtb
