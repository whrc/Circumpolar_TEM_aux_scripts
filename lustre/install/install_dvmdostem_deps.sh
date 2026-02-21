#!/bin/bash
set -euo pipefail

# --------- Config ---------
BOOST_VER=1.80.0
HDF5_VER=1.10.9
NETCDF_VER=4.4.1.1

LUSTRE_HOME=/mnt/exacloud/lustre
SW=$LUSTRE_HOME/software
SRC=$LUSTRE_HOME/src/dvmdostem-deps

BOOST_PREFIX=$SW/boost/$BOOST_VER
HDF5_PREFIX=$SW/hdf5/$HDF5_VER
NETCDF_PREFIX=$SW/netcdf-c/$NETCDF_VER

mkdir -p "$SRC" "$BOOST_PREFIX" "$HDF5_PREFIX" "$NETCDF_PREFIX"

# --------- Load modules (adjust to your site) ---------
# Makes `module` available in non-interactive shells
source /etc/profile.d/z00_lmod.sh || true

module purge || true
module load openmpi || true

# Ensure we build with the same MPI we will run with
export CC=mpicc
export CXX=mpicxx
export FC=mpifort || true

export CFLAGS="-O2 -fPIC"
export CXXFLAGS="-O2 -fPIC"
export MAKEFLAGS="-j$(nproc)"

cd "$SRC"

echo "==> Building Boost $BOOST_VER into $BOOST_PREFIX"
if [ ! -f "boost_1_80_0.tar.gz" ]; then
  wget -O boost_1_80_0.tar.gz "https://archives.boost.io/release/1.80.0/source/boost_1_80_0.tar.gz"
fi
rm -rf boost_1_80_0
tar -xzf boost_1_80_0.tar.gz
pushd boost_1_80_0
./bootstrap.sh --prefix="$BOOST_PREFIX"
# Enable Boost.MPI build using the MPI compiler wrapper
echo "using mpi : $CC ;" >> project-config.jam
./b2 install
popd

echo "==> Building HDF5 $HDF5_VER into $HDF5_PREFIX"
if [ ! -f "hdf5-1.10.9.tar.gz" ]; then
  wget -O hdf5-1.10.9.tar.gz "https://support.hdfgroup.org/ftp/HDF5/releases/hdf5-1.10/hdf5-1.10.9/src/hdf5-1.10.9.tar.gz"
fi
rm -rf hdf5-1.10.9
tar -xzf hdf5-1.10.9.tar.gz
pushd hdf5-1.10.9
CC="$CC" ./configure \
  --prefix="$HDF5_PREFIX" \
  --enable-parallel \
  --enable-shared \
  --disable-static \
  CFLAGS="$CFLAGS"
make
make install
popd

echo "==> Building NetCDF-C $NETCDF_VER into $NETCDF_PREFIX"
if [ ! -f "v4.4.1.1.tar.gz" ]; then
  wget -O v4.4.1.1.tar.gz "https://github.com/Unidata/netcdf-c/archive/refs/tags/v4.4.1.1.tar.gz"
fi
rm -rf netcdf-c-4.4.1.1
tar -xzf v4.4.1.1.tar.gz
pushd netcdf-c-4.4.1.1
# Disable DAP to reduce external runtime deps (curl)
CC="$CC" \
CPPFLAGS="-I$HDF5_PREFIX/include" \
LDFLAGS="-L$HDF5_PREFIX/lib" \
./configure \
  --prefix="$NETCDF_PREFIX" \
  --enable-parallel \
  --disable-dap \
  --enable-shared \
  --disable-static
make
make install
popd

echo "DONE: Boost/HDF5/NetCDF-C installed under:"
echo "  $BOOST_PREFIX"
echo "  $HDF5_PREFIX"
echo "  $NETCDF_PREFIX"
