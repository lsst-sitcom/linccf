#!/bin/bash

# Directory to save the file
output_dir="/sdf/data/rubin/u/olynn/sdss/"

urls=(
#    "http://das.sdss.org/va/qsocat/dr7qso.fit.gz"
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