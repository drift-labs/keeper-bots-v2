#!/bin/bash

# URL of the zip file
url="https://github.com/jup-ag/jupiter-swap-api/releases/download/v6.0.5/jupiter-swap-api-x86_64-unknown-linux-gnu.zip"


FILE_DIR="$(dirname "$(realpath "$0")")"

# Directory where the zip file will be downloaded and unzipped
dir=$FILE_DIR

# Download the zip file using wget
wget -P "$dir" "$url"

# Extract the name of the zip file from the URL
zip_file=$(basename "$url")

# Full path to the downloaded zip file
zip_path="$dir/$zip_file"

# Unzip the zip file
unzip "$zip_path" -d "$dir"

# Remove the zip file after unzipping
rm "$zip_path"

# Assume that the unzipped file is a script with a .sh extension
# Extract the name of the script from the name of the zip file
script="${zip_file%.zip}"

# Full path to the script
script_path="$dir/jupiter-swap-api"

# Make the script executable
chmod +x "$script_path"
