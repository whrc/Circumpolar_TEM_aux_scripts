help([[
HDF5 1.10.9 (parallel) built with OpenMPI wrapper (mpicc)
]])

whatis("Name: hdf5")
whatis("Version: 1.10.9")

local root = "/mnt/exacloud/lustre/software/hdf5/1.10.9"

setenv("HDF5_ROOT", root)
setenv("HDF5_DIR", root)

prepend_path("PATH", pathJoin(root, "bin"))
prepend_path("CPATH", pathJoin(root, "include"))
prepend_path("LIBRARY_PATH", pathJoin(root, "lib"))
prepend_path("LD_LIBRARY_PATH", pathJoin(root, "lib"))
prepend_path("PKG_CONFIG_PATH", pathJoin(root, "lib", "pkgconfig"))
