#!/bin/bash
# download and copy installation software to AWS S3 bucket

usage() {
    printf "usage: $0 s3://mybucket/destination-key\n"
    exit 1
}

# exit on any failure
set -e

# check have one argument that starts with "s3://"
[[ $# -eq 1 && ${1:0:5} == "s3://" ]] || usage

s3_uri=$1

# create a temporary directory
tmp=tmp-$( date "+%s" )
mkdir $tmp
cd $tmp

# STAR
wget -O STAR-2.5.2a.tar.gz https://github.com/alexdobin/STAR/archive/2.5.2a.tar.gz

# subread 
wget -O subread-1.5.0-p3-Linux-x86_64.tar.gz https://sourceforge.net/projects/subread/files/subread-1.5.0-p3/subread-1.5.0-p3-Linux-x86_64.tar.gz/download

# picard tools
wget https://github.com/broadinstitute/picard/releases/download/2.4.1/picard-tools-2.4.1.zip

# sam tools
wget -O samtools-1.3.1.tar.bz2 https://sourceforge.net/projects/samtools/files/samtools/1.3.1/samtools-1.3.1.tar.bz2/download

# prinseq
wget -O prinseq-lite-0.20.4.tar.gz https://sourceforge.net/projects/prinseq/files/standalone/prinseq-lite-0.20.4.tar.gz/download

# trim galore
wget http://www.bioinformatics.babraham.ac.uk/projects/trim_galore/trim_galore_v0.4.1.zip

# trimmomatic
wget http://www.usadellab.org/cms/uploads/supplementary/Trimmomatic/Trimmomatic-0.36.zip

cd ..
aws s3 sync $tmp $s3_uri
rm -r $tmp

printf "\nINSTALLATION FILES SUCCESSFULLY DOWNLOADED AND COPIED TO S3\n"
