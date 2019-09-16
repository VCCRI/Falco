#!/bin/bash
###############################################################################
# designed for use with aws instance with ssd (called from --user-data option #
# mounts ssd to /ssd & creates directories for DNA variant calling pipeline   #
#                                                                             #
###############################################################################

s3_software_install=s3://[YOUR-BUCKET]/...
aws_region=us-west-2

create_dir() {
    dir=$1
    # allow for case where image already has dir
    sudo mkdir -p $dir
    check_status "mkdir -p $dir"
    sudo chmod a+rwx $dir
    check_status "chmod $dir"
    sudo chgrp ec2-user $dir
    check_status "chgrp $dir"
    sudo chown ec2-user $dir
    check_status "chown $dir"
}

function unzip_files() {
    # unzip any .gz files in current directory or any subdirectories
    # determine if there are any .gz files; note that without this test, the xargs command would fail with a null input
    zip_files=$( find -L . -name "*.gz" -print0 )
    if [ "$zip_files" != "" ] ; then
        # unzip all the .gz files using as many processors as possible
        find -L . -name "*.gz" -print0 | xargs -P0 -0 gunzip
    fi
}

#copy data to newly mounted drive
mount_dir=/
create_dir $mount_dir/app

set -e
set -o pipefail

# update system software
sudo yum update -y --skip-broken

pushd /app > /dev/null

# copy install software
aws s3 cp $s3_software_install . --recursive --region=$aws_region

# give rights to ec2-user
for f in * ; do
    sudo chgrp ec2-user $f
    sudo chown ec2-user $f
done

# Install STAR and its' dependencies
sudo yum install make gcc-c++ glibc-static -y

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

# Install HISAT2
unzip hisat2*.zip
hisat_dir=$( find . -maxdepth 1 -type d -name "hisat2*")
ln -s $hisat_dir hisat

# Install HTSeq
sudo yum install python27-devel python27-numpy python27-matplotlib python27-Cython -y
sudo pip install pysam
sudo pip install htseq

# Install samtools
sudo yum install zlib-devel ncurses-devel ncurses bzip2-devel xz-devel -y
tar -xjf samtools*.tar.bz2
sam_dir=$( find . -maxdepth 1 -type d -name "samtools*" )
pushd $sam_dir > /dev/null
make
sudo make install
popd > /dev/null
ln -s $sam_dir samtools

# Install htslib
hts_dir=$( find $sam_dir -maxdepth 1 -type d -name "htslib-*" )
pushd $hts_dir > /dev/null
make
sudo make install
popd > /dev/null

# Install picard_tools
# Note: latest version of picard_tools come as a jar file. We do not need to do anything.
mkdir picard-tool
mv picard.jar picard-tool/

# Install stringtie
tar -xzf stringtie*.tar.gz
stringtie_dir=$( find . -maxdepth 1 -type d -name "stringtie*")
ln -s $stringtie_dir stringtie

# Install scallop
tar -xzf scallop-*_linux_x86_64.tar.gz
scallop_dir=$( find . -maxdepth 1 -type d -name "scallop*" )
ln -s $scallop_dir scallop

# Install gffcompare
tar -xzf gffcompare*.tar.gz
gffcompare_dir=$( find . -maxdepth 1 -type d -name "gffcompare*")
ln -s $gffcompare_dir gffcompare

# INSTALL CUSTOM PRE-PROCESSING TOOLS BELOW

# trim galore
tg=trim_galore
unzip trim_galore*.zip
tg_path=$( find . -name $tg )
ln -s $tg_path $tg

# trimmomatic
unzip Trimmomatic*.zip
tm=$( find . -name trimmomatic*.jar )
ln -s $tm ${tm##*/}
# hardcoded
ln -s Trimmomatic-0.36/adapters/NexteraPE-PE.fa NexteraPE-PE.fa

# prinseq
ps=prinseq-lite.pl
tar -xzf prinseq-lite*.tar.gz
ps_path=$( find . -name "$ps" )
ln -s $ps_path $ps

# install cutadapt
sudo pip install cutadapt

# -------------------------------------------------------------
# no longer in /app
popd > /dev/null

mkdir /mnt/output

# Install python dependencies for framework
sudo yum install python35 -y
sudo pip install pandas boto3 ascii_graph pysam
sudo python3 -m pip install pandas boto3 ascii_graph pysam

# Install java8
sudo yum install java-1.8.0-openjdk.x86_64 -y

# install htop
sudo yum install htop -y


