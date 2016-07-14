#!/bin/bash
# copies reference files from S3 & unzips
# input args:
#   $1 - AWS S3 URI for genome reference location
#   $2 - AWS S3 URI for STAR reference location

# want to terminate on error
set -e
set -o pipefail

function unzip_files() {
    # unzip any .gz files in current directory or any subdirectories
    # determine if there are any .gz files; note that without this test, the xargs command would fail with a null input
    zip_files=$( find -L . -name "*.gz" -print0 )
    if [ "$zip_files" != "" ] ; then
        # unzip all the .gz files using as many processors as possible
        find -L . -name "*.gz" -print0 | xargs -P0 -0 gunzip
    fi
}

ref_dir=/mnt/ref
genome_dir=genome_ref
star_dir=star_ref

mkdir $ref_dir
pushd $ref_dir > /dev/null

# Genome Ref
aws s3 sync $1 $genome_dir
pushd $ref_dir/$genome_dir > /dev/null
unzip_files
popd > /dev/null

# STAR Ref
aws s3 sync $2 $star_dir
pushd $ref_dir/$star_dir > /dev/null
unzip_files
popd > /dev/null
