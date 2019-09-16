#!/bin/bash
# an example of a user-provided pre-processing script
# pre-processing done with prinseq, cutadapt, and trim-galore
# expects either 1 or 2 input arguments - being the fastq file names (pathless)
# The user is required to replace the supplied file with the pre-processing
# output (i.e. the fastq that has been through pre-processing)
# NOTE that this file is used by the Falco framework (in particular, this file is executed as a Hadoop streaming job)
# such that the input data (the list of file names) is streamed into this file

# this file includes example code that could be used for paired-end data

prinseq=/app/prinseq-lite.pl
trim_galore=/app/trim_galore

# function to deal with terminal errors
exit_msg() {
    printf "$0 program terminated.  %s\n" "$1"
    exit 1
}

# check that there are 2 arguments to this program - paired end
[ $# -eq 2 ] || exit_msg "must be 2 arguments"
fq_1="$1"
fq_2="$2"

# return from script if not paired-end (as this is a pe example script)
[ -f $fq_1 ] || exit_msg "Unable to locate read 1 file: $fq_1"
[ -f $fq_2 ] || exit_msg "Unable to locate read 1 file: $fq_2"

[ -f $prinseq ] || exit_msg "Couldn't find prinseq: $prinseq"
[ -f $trim_galore ] || exit_msg "Couldn't find TrimGalore: $trim_galore"

# now ready to start QC processing with fq_1 & fq_2
# which are working copies residing in the current "work" directory
# assumes input files include the character string ".fastq"
prefix=${fq_1%.fastq*}
prefix_1=${fq_1%.fastq*}
prefix_2=${fq_2%.fastq*}
# prinseq
perl $prinseq -min_len 30 -trim_left 10 -trim_qual_right 25 \
    -lc_method entropy \-lc_threshold 65 -fastq $fq_1 \
    -fastq2 $fq_2 -out_bad null -out_good $prefix
[ $? -eq 0 ] || exit_msg "prinseq command failed"
# this will produce possibly a _singletons file - ignore at this stage
[ -f ${prefix}_1.fastq ] || exit_msg \
    "prinseq - unable to locate output: ${prefix}_1.fastq"
[ -f ${prefix}_2.fastq ] || exit_msg \
    "prinseq - unable to locate output: ${prefix}_2.fastq"
# replace existing working fastq 
mv ${prefix}_1.fastq $fq_1
mv ${prefix}_2.fastq $fq_2

# cutadapt
cutadapt -e 0.15 -m 30 -o ${prefix}_ca1.fastq -p ${prefix}_ca2.fastq $fq_1 $fq_2
[ $? -eq 0 ] || exit_msg "cutadapt command failed"
[ -f ${prefix}_ca1.fastq ] || exit_msg \
    "cutadapt - unable to locate output: ${prefix}_ca1.fastq"
[ -f ${prefix}_ca2.fastq ] || exit_msg \
    "cutadapt - unable to locate output: ${prefix}_ca2.fastq"
# replace existing working fastq 
mv ${prefix}_ca1.fastq $fq_1
mv ${prefix}_ca2.fastq $fq_2

# TrimGalore
time $trim_galore --stringency 1 $fq_1 $fq_2
[ $? -eq 0 ] || exit_msg "trimgalore command failed"
[ -f ${prefix_1}_trimmed.fq ] || exit_msg \
    "trim_galore - unable to locate output: ${prefix_1}_trimmed.fq"
[ -f ${prefix_2}_trimmed.fq ] || exit_msg \
    "trim_galore - unable to locate output: ${prefix_2}_trimmed.fq"
# IMPORTANT: replace existing working fastq
mv ${prefix_1}_trimmed.fq $fq_1
mv ${prefix_2}_trimmed.fq $fq_2

