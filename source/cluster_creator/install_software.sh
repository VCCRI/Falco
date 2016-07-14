#!/bin/bash
# used as a "bootstrap" script for an EMR cluster
# installs software - in a version agnostic manner where possible
# ASSUMPTION: only one version of each software package is available locally

set -e
set -o pipefail

sudo yum update -y

mkdir /mnt/app
pushd /mnt/app > /dev/null

aws s3 cp $1 . --recursive

# Install STAR and its' dependencies
sudo yum install make -y
sudo yum install gcc-c++ -y
sudo yum install glibc-static -y

# STAR
tar -xzf STAR*.tar.gz
star_path=$( find . -name "STAR"|grep -E "/Linux_x86_64/" )
# symbolic link to the STAR directory (rather than to the executable itself)
ln -s ${star_path%STAR} STAR

sudo yum install python-devel numpy python-matplotlib -y

# Install subread (featureCount)
tar -xzf subread*.tar.gz
fc=$( find -name "featureCounts"|grep bin )
sr_path=${fc%featureCounts}
ln -s $sr_path subread

# Install samtools
tar -xjf samtools*.tar.bz2
sam_dir=$( find . -maxdepth 1 -type d -name "samtools*" )
pushd $sam_dir > /dev/null
make
sudo make install
popd > /dev/null

# Install htslib
hts_dir=$( find $sam_dir -maxdepth 1 -type d -name "htslib-*" )
pushd $hts_dir > /dev/null
make
sudo make install

popd > /dev/null

# Install picard_tools
unzip picard-tools*.zip
pic_jar=$( find . -name picard.jar )
pic_path=${pic_jar%picard.jar}
ln -s $pic_path picard-tools

# trim galore
tg=trim_galore
unzip trim_galore*.zip
tg_path=$( find . -name $tg )
ln -s $tg_path $tg

# trimmomatic
unzip Trimmomatic*.zip
tm=$( find . -name trimmomatic*.jar )
ln -s $tm ${tm##*/}

# prinseq
ps=prinseq-lite.pl
tar -xzf prinseq-lite*.tar.gz
ps_path=$( find . -name "$ps" )
ln -s $ps_path $ps

# -------------------------------------------------------------
# no longer in /mnt/app
popd > /dev/null

mkdir /mnt/output

# Install python dependencies for framework
sudo pip install pandas
sudo pip install boto3
sudo python3 -m pip install boto3

# Install java8
sudo yum install java-1.8.0-openjdk.x86_64 -y

# install cutadapt
sudo pip install cutadapt

# install htop
sudo yum install htop -y
