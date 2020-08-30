#!/bin/bash

main() {

    set -x
    echo "== Merge Counts =="


    # Install dependencies
    echo "  [*] Downloading dependencies"
    sudo apt-get update
    sudo apt install build-essential \
                     zlib1g-dev \
                     libncurses5-dev \
                     libgdbm-dev \
                     libnss3-dev \
                     libssl-dev \
                     libreadline-dev \
                     libffi-dev \
                     libsqlite3-dev \
                     wget \
                     libbz2-dev -y

    # Install Python 3.8 (required for stjudecloud-merge-counts)
    echo "  [*] Installing Python3.8"
    wget https://www.python.org/ftp/python/3.8.0/Python-3.8.0.tgz
    tar -xf Python-3.8.0.tgz
    cd Python-3.8.0/
    ./configure --enable-optimizations
    sudo make -j `nproc`
    sudo make altinstall
    cd ~

    # Install stjudecloud-merge-counts and add to PATH
    echo "  [*] Installing stjudecloud-merge-counts"
    pip3.8 install stjudecloud-merge-counts --user
    export PATH="${PATH}:/home/dnanexus/.local/bin"

    # Download counts files
    echo "  [*] Downloading counts files."
    echo "Counts files: ${counts[@]}"
    dx-download-all-inputs --parallel 1>/dev/null 2>/dev/null

    # Merge the counts matrix
    echo "  [*] Merging counts matrix"
    output_filename="counts-matrix.${output_filetype}"
    stjudecloud-merge-counts recursive -o "${output_filename}" \
                                       -t "${output_filetype}" \
                                       $(find in/ -type f)

    # Create the attributes matrix
    echo "  [*] Creating attributes matrix"
    echo "Hello, world!" > attributes_matrix

    # Upload and tag results
    echo "  [*] Upload and tag results"
    ls -lah "${output_filename}" "attributes_matrix"
    counts_matrix=$(dx upload "${output_filename}" --brief)
    echo "Counts Matrix: $counts_matrix"
    attributes_matrix=$(dx upload attributes_matrix --brief)
    echo "Attributes Matrix: $attributes_matrix"
    dx-jobutil-add-output counts_matrix "$counts_matrix" --class=file
    dx-jobutil-add-output attributes_matrix "$attributes_matrix" --class=file
    echo "  [*] Finished."
}
