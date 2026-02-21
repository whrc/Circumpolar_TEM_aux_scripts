help([[
NetCDF-C 4.4.1.1 built with parallel I/O support
]])

whatis("Name: netcdf-c")
whatis("Version: 4.4.1.1")

depends_on("hdf5/1.10.9")

local root = "/mnt/exacloud/lustre/software/netcdf-c/4.4.1.1"

setenv("NETCDF_ROOT", root)
setenv("NETCDF_DIR", root)

prepend_path("PATH", pathJoin(root, "bin"))
prepend_path("CPATH", pathJoin(root, "include"))
prepend_path("LIBRARY_PATH", pathJoin(root, "lib"))
prepend_path("LD_LIBRARY_PATH", pathJoin(root, "lib"))
prepend_path("PKG_CONFIG_PATH", pathJoin(root, "lib", "pkgconfig"))
