#!/bin/bash
# copies reference files from S3 & unzips
# input args:
#   $1 - AWS S3 URI for location containing genome reference, star index and hisat index

# want to terminate on error
set -e
set -o pipefail

ref_dir=/mnt/ref

aws s3 sync $1 $ref_dir

pushd $ref_dir
# unzip any .gz files in current directory or any subdirectories
# determine if there are any .gz files; note that without this test, the xargs command would fail with a null input
zip_files=$( find -L . -name "*.gz" -print0 )
if [ "$zip_files" != "" ] ; then
    # unzip all the .gz files using as many processors as possible
    find -L . -name "*.gz" -print0 | xargs -0 -n1 -P0 gunzip
fi
popd

