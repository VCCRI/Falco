#!/bin/bash
# download some sample files for the tutorial & upload them
# to an AWS S3 location
# args:
#   $1 - the S3 URI for uploading the sample data
#       e.g. s3://your-bucket/falco-tutorial/data
# exit on any error
set -e

usage() {
    printf "usage: $0 <AWS S3 URI>\n"
    printf "e.g. $0 s3://your-bucket/falco-tutorial/data\n"
    exit 1
}

# check for valid usage
[ $# -ne 1 ] && usage
[ "${1:0:5}" != "s3://" ] && usage

# variables
s3_uri=$1
file_listing=falco-tutorial-files.txt
data_dir=falco-tutorial-data

files=(ftp://ftp.sra.ebi.ac.uk/vol1/fastq/SRR197/006/SRR1974616/SRR1974616_1.fastq.gz
ftp://ftp.sra.ebi.ac.uk/vol1/fastq/SRR197/006/SRR1974616/SRR1974616_2.fastq.gz
ftp://ftp.sra.ebi.ac.uk/vol1/fastq/SRR197/006/SRR1974816/SRR1974816_1.fastq.gz
ftp://ftp.sra.ebi.ac.uk/vol1/fastq/SRR197/006/SRR1974816/SRR1974816_2.fastq.gz
ftp://ftp.sra.ebi.ac.uk/vol1/fastq/SRR197/000/SRR1974980/SRR1974980_1.fastq.gz
ftp://ftp.sra.ebi.ac.uk/vol1/fastq/SRR197/000/SRR1974980/SRR1974980_2.fastq.gz
ftp://ftp.sra.ebi.ac.uk/vol1/fastq/SRR197/001/SRR1974811/SRR1974811_1.fastq.gz
ftp://ftp.sra.ebi.ac.uk/vol1/fastq/SRR197/001/SRR1974811/SRR1974811_2.fastq.gz
ftp://ftp.sra.ebi.ac.uk/vol1/fastq/SRR197/003/SRR1974793/SRR1974793_1.fastq.gz
ftp://ftp.sra.ebi.ac.uk/vol1/fastq/SRR197/003/SRR1974793/SRR1974793_2.fastq.gz)

# remove file listing if it exists
[ -f $file_listing ] && rm $file_listing
# create new file listing file
for f in ${files[@]} ; do echo $f >> $file_listing ; done
# create data directory - program should fail if it already exists
mkdir $data_dir
cd $data_dir
# use as many processors as is available
# ignore stderr (file download progress)
echo "STARTING: file download"
xargs -P0 -I fname wget fname < ../$file_listing 2>/dev/null
# upload to S3 - need to have S3 URI
echo "STARTING: sync to AWS S3"
aws s3 sync . $s3_uri

# tidy up
cd ..
rm -r $data_dir
rm $file_listing
echo "SUCCESS: tutorial files successfully uploaded to AWS S3"
