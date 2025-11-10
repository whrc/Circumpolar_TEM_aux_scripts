#!/bin/bash
# Quick launcher for the circumpolar viewer with lat/lon ticks

echo "Starting Circumpolar Viewer with Lat/Lon Axis Labels..."
echo "==========================================================="
echo ""
echo "Features:"
echo "  ✓ Latitude/Longitude grid overlay"
echo "  ✓ Lat/Lon tick labels on X and Y axes"
echo "  ✓ Fast image rendering"
echo "  ✓ Interactive zoom and animation"
echo ""
echo "The browser will open automatically at http://localhost:5006/"
echo "Press Ctrl+C to stop the server"
echo ""

python plot_bokeh_circumpolar_latlon.py ../merge/Alaska/merged/GPP_ssp5_8_5_mri_esm2_0_sc_yearly.nc


