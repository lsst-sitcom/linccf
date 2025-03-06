#!/bin/bash

# Note that we actually can skip downloading the standard sdss dr7 quasar catalog
# (at http://das.sdss.org/va/qsocat/dr7qso.fit.gz), as the "Additional Properties" catalog contains 
# all the data we'll need.

# Directory to save the file
output_dir="/data3/epyc/data3/hats/raw/sdss_dr7sqo_props"

# SDSS DR7 QSO (Additional Properties)
urls=(
    "https://das.sdss.org/va/qso_properties_dr7/data/catalogs/dr7_bh_June_2010.fits.gz"
)

for url in "${urls[@]}"; do
    echo "Downloading: $url"

    # Use wget to download the file
    wget -P "$output_dir" "$url"
    
    # Check if the download was successful
    if [ $? -eq 0 ]; then
        echo "Download successful."
    else
        echo "Download failed."
        exit 1
    fi
done