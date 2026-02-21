help([[
Boost 1.80.0 built on Lustre
]])

whatis("Name: boost")
whatis("Version: 1.80.0")

local root = "/mnt/exacloud/lustre/software/boost/1.80.0"

setenv("BOOST_ROOT", root)
prepend_path("CPATH", pathJoin(root, "include"))
prepend_path("LIBRARY_PATH", pathJoin(root, "lib"))
prepend_path("LD_LIBRARY_PATH", pathJoin(root, "lib"))
prepend_path("PKG_CONFIG_PATH", pathJoin(root, "lib", "pkgconfig"))
