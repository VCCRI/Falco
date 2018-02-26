#!/bin/bash
# A script used to process joined, zipped fastq files
# through map-reduce.  The processing is via a user-supplied
# QC script.  All processing is done via a "scratch" directory
# which is removed after use for each file.
#
# Processing from stdin.
#
# Input is zipped, split pe or se fastq files - where
# each record (every 4 lines in a normal fastq file) is concatinated
# into 1 line & pe records are also joined:
#
# line1 \t line2 \t line3 \t line4 [\t line1 \t ...line4]
#
# The input is expected to be a mainifest file, where individual
# file names like: SRR1974543_1_part0.fastq.gz
# manifest file will only have 1 filename per part (as pe content is combined)
#

# function to deal with terminal errors
exit_msg() {
    printf "$0 program terminated.  %s\n" "$1"
    exit 1
}

input=""
output=""
region=""
user_script=""
#work_dir=$( pwd )
input_is_s3=true
output_is_s3=true

# get input args
while [[ $# > 0 ]]
do
    arg="$1"
    case $arg in
        --input|-i)
            input="$2"
            shift
            shift
            ;;
        --output|-o)
            output="$2"
            shift
            shift
            ;;
        --region|-r)
            region="$2"
            shift
            shift
            ;;
        --user-script|-u)
            user_script="$2"
            shift
            shift
            ;;
        --additional-files|-a)
            additional_files="$2"
            shift
            shift
            ;;
        *)
            # unknown option - exit program
            exit_msg "Unknown option: ${1}."
            ;;
    esac
done
# check that we have all the input parms
# Note that --additional-files is optional
for var in input output user_script ; do
   [ "${!var}" != "" ] || exit_msg "Missing input parameter: $var"
done

# check input/output to see if on s3 file system (or dfs)
[ "${input:0:5}" == "s3://" ] || input_is_s3=false
[ "${output:0:5}" == "s3://" ] || output_is_s3=false

if $input_is_s3 ; then
    var=region
    [ "${!var}" != "" ] || exit_msg "Missing input parameter: $var"
fi

# create the output dir if using hdfs
if ! $output_is_s3 ; then
    hdfs dfs -mkdir -p $output
    [ $? -eq 0 ] || exit_msg "Unable to create hdfs output directory: $output"
fi

#input is streamed - read the input
while read f ; do
    # preprocessing fastq files to emulate QC steps as outlined in 
    # http://www.ncbi.nlm.nih.gov/pmc/articles/PMC4466750/#d35e297
    #
    # Assumptions
    #   (1) one filename per line of input
    #   (1a) either pe or se data
    #   (1b) file formatted in "pre-processed format":
    #       - 1 record per line (iso the normal 1 record per 4 lines)
    #       - pe files - matching records on 1 line
    #       - lines in a record separated by '\t'
    #       - record in pe file separated by '\t|\t'
    #   (2) zipped files end in .gz & are compressed with gnu zip
    #   (3) the name of the fastq file will either be <prefix>(.fq|.fastq)[.gz]
    #       - e.g. SRR1974543_1_part0.fastq.gz
    #       - in this e.g., the id is SRR1974543_1
    #   (4) apart from manifest, all file/dir variables should use full paths
    #   (5) working dir is current dir

    # atm assuming input files are zipped fastq files
    # check that file name ends in ".fastq.gz"
    str=".fastq.gz"
    str_len=${#str}
    [ "${f: -$str_len}" == "$str" ] || exit_msg "Invalid input file - expecting $str file: $f" 

    # set file id
    id=${f%.fastq.gz}
    # single-ended id
    se_id=${id}.fastq

    # set up some variables to store the full path names for the file(s)
    fq_1=${id}_1.fastq
    fq_2=${id}_2.fastq
    
    # make a scratch directory with unique id to do the working
    # add container_id to handle case of pre-emptive execution by hadoop
    # Note that $CONTAINER_ID is a Yarn variable
    scratch=scratch-${id}-$CONTAINER_ID
    mkdir $scratch
    # copy any user files to the scratch directory
    cp $user_script $scratch
    if [ "$additional_files" != "" ] ; then
        # additional_files should be a comma separated string of file names
        IFS="," read -r -a files_array <<< "$additional_files"
        # enable extended globbing to help with trimming whitespace
        shopt -s extglob
        for fname in ${files_array[@]} ; do
            # trim leading & trailing whitespace
            fname=${fname##+([[:space:]])}; fname=${fname%%+([[:space:]])}
            # copy the file to the scratch directory
            if [[ -n "$fname" && -f "$fname" ]] ; then
                cp $fname $scratch
            fi
        done
        # disable extended globbing
        shopt -u extglob 
    fi
    cd $scratch || exit_msg "Unable to change to scratch dir: $scratch"
    # catch situation where we might have some sort of re-run & file exists
    for fname in $fq_1 $fq_2 ; do
        [ -f $fname ] && rm $fname
    done

    # get a local copy of the input data
    # handle case where input dir has file_sep as last char or not
    file_sep="/"
    [ "${input: -1}" == "$file_sep" ] && file_sep=""
    if $input_is_s3 ; then
        aws s3 cp $input$file_sep$f . --region $region
    else
        hdfs dfs -get $input$file_sep$f .
    fi
    [ $? -eq 0 ] || exit_msg "unable to download $f from $input"
    
    # unzip & split file
    # first check if se or pe
    # assumes tabs separate lines in a record
    # and "\t" separates matching records in pe files
    SE_FIELDS=4
    PE_FIELDS=8
    # file names to pass through to user script
    # - either se or pe
    file_names="$fq_1 $fq_2"
    paired=true
    # look at first line of input file to determine if pe or se
    num_fields=$( zcat $f|head -1|awk -F "\t" '{print NF}' )
    if [ $num_fields -eq $SE_FIELDS ] ; then
        paired=false
        file_names=$se_id
    elif [ $num_fields -ne $PE_FIELDS ] ; then
        exit_msg "malformed input file $f"
    fi
        
    if $paired ; then
        # "\t" is the separator between pe file matching records
        # NOTE that the single redirection symbol ">" is used here
        # as awk only opens file once & redirects the output to that
        # file (appending) - hence no need to use ">>"
        zcat $f|awk -F "\t" -v _fq_1="$fq_1" -v _fq_2="$fq_2" '{\
                print $1 "\n" $2 "\n" $3 "\n" $4 > _fq_1; \
                print $5 "\n" $6 "\n" $7 "\n" $8 > _fq_2}'
    else
        zcat $f|awk -F "\t" -v _fq_1="$se_id" '{\
                print $1 "\n" $2 "\n" $3 "\n" $4 > _fq_1}'
    fi

    # Call user script - each file name is a separate argument to the user script
    ./$user_script $file_names
    [ $? -eq 0 ] || exit_msg "Unsuccessful call to $user_script for file(s): $file_names"

    # need to process the files before zipping (e.g. join files &/or join lines)
    # note that the output is named the same as the original input
    if $paired ; then
        # qc may return files of uneven length (e.g. may remove a read from one of the 
        # paired files due to quality (but leave the matching read in the other pe file
        # FNR==NR {...} only executes for first file (FNR = overall line #; NR = file line #)
        # uses associative array "a" to store record by key=sequence id
        # NOTE that sequence id is assumed to be first string up to first blank
        # didn't sort - as we are constructing a combined pair file that will
        # be in the same order
        awk 'FNR==NR {a[$1]=$0; next}{if (_1_rec=a[$1]){print _1_rec "\t" $0}}' \
            <( paste - - - - < $fq_1 ) <( paste - - - - < $fq_2 ) | gzip --fast > $f
    else
        # single file
        awk '{printf("%s%s",$0,(NR%4==0)?"\n":"\t")}' $se_id |gzip --fast > $f
    fi

    # upload data & remove scratch directory
    file_sep="/"
    [ "${output: -1}" == "$file_sep" ] && file_sep=""
    if $output_is_s3 ; then
        aws s3 cp $f $output$file_sep$f --region $region
    else
        # assume non-s3 output is hdfs
        hdfs dfs -put $f $output$file_sep$f
    fi
    [ $? -eq 0 ] || exit_msg "unable to upload $f to $output"

    # remove temporary files
    cd ..
    rm -r $scratch
done
