#!/bin/bash

# Directory to save the file(s)
output_dir="/sdf/data/rubin/u/olynn/AGNs/raw_data"

# Raw data
# See: https://quasars.org/milliquas.htm
urls=(
    #"https://quasars.org/milliquas.zip"
    "https://quasars.org/milliquas.fits.zip"
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