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
wget -O STAR-2.5.4b.tar.gz https://github.com/alexdobin/STAR/archive/2.5.4b.tar.gz

# HISAT2
wget -O hisat2-2.1.0.zip ftp://ftp.ccb.jhu.edu/pub/infphilo/hisat2/downloads/hisat2-2.1.0-Linux_x86_64.zip

# subread
wget -O subread-1.6.0-Linux-x86_64.tar.gz https://sourceforge.net/projects/subread/files/subread-1.6.0/subread-1.6.0-Linux-x86_64.tar.gz/download

# stringtie
wget -O stringtie-1.3.3b.tar.gz http://ccb.jhu.edu/software/stringtie/dl/stringtie-1.3.3b.Linux_x86_64.tar.gz

# Scallop
wget https://github.com/Kingsford-Group/scallop/releases/download/v0.10.2/scallop-0.10.2_linux_x86_64.tar.gz

# gffcompare
wget -O gffcompare.tar.gz http://ccb.jhu.edu/software/stringtie/dl/gffcompare-0.10.1.Linux_x86_64.tar.gz

# picard tools
wget -O picard.jar https://github.com/broadinstitute/picard/releases/download/2.17.10/picard.jar

# sam tools
wget -O samtools-1.7.tar.bz2 https://github.com/samtools/samtools/releases/download/1.7/samtools-1.7.tar.bz2

# prinseq
wget -O prinseq-lite-0.20.4.tar.gz https://sourceforge.net/projects/prinseq/files/standalone/prinseq-lite-0.20.4.tar.gz/download

# trim galore
wget -O trim_galore_v0.4.5.zip https://github.com/FelixKrueger/TrimGalore/archive/0.4.5.zip

# trimmomatic
wget http://www.usadellab.org/cms/uploads/supplementary/Trimmomatic/Trimmomatic-0.36.zip

cd ..
aws s3 sync $tmp $s3_uri
rm -r $tmp

printf "\nINSTALLATION FILES SUCCESSFULLY DOWNLOADED AND COPIED TO S3\n"
