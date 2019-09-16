import argparse
import sys
from operator import add
import os
import shlex
import shutil
from subprocess import Popen, PIPE
from pyspark import SparkContext, SparkConf
import pyspark.serializers
import subprocess
import boto3
import re

global parser_result

if sys.version > "3.4":
    pyspark.serializers.protocol = 4

APPLICATION_FOLDER = "/app"
GENOME_REFERENCES_FOLDER = "/mnt/ref"
TEMP_OUTPUT_FOLDER = "/mnt/output"
HDFS_TEMP_OUTPUT_FOLDER = "/tmp/sam_chunks"


#################################
#  File splitting
#################################


def split_interleaved_file(file_prefix, file_content, output_dir):
    """
    Unpacks an interleaved file into the standard FASTQ format
    :param file_prefix: the prefix of the file name
    :param file_content: the lines of content from the input file
    :param output_dir: the location to store the unpacked files
    :return: a tuple with first element being a list of output file names
    (1 for se, 2 for pe); 2nd element a boolean flag - True if pe data,
    False otherwise
    """
    fastq_line_count_se = 4
    fastq_line_count_pe = 8
    paired_reads = False
    output_file_names = []

    file_prefix = output_dir + "/" + file_prefix
    output_file = file_prefix + "_1.fq"
    output_file_names.append(output_file)
    output_file_writer = open(output_file, 'w')

    count = 0
    for line in file_content.strip().split("\n"):
        # In the first line, check if it's paired or not
        if count == 0 and len(line.strip().split("\t")) == fastq_line_count_pe:
            paired_reads = True
            output_file_pair = file_prefix + "_2.fq"
            output_file_names.append(output_file_pair)
            output_pair_writer = open(output_file_pair, 'w')

        if paired_reads:
            parts = line.strip().split("\t")

            if len(parts) != fastq_line_count_pe:
                continue

            read_one = parts[:fastq_line_count_se]
            read_two = parts[fastq_line_count_se:]
            output_file_writer.write("\n".join(read_one) + "\n")
            output_pair_writer.write("\n".join(read_two) + "\n")
        else:
            output_file_writer.writelines(line.strip().replace("\t", "\n") + "\n")

        count += 1

    output_file_writer.close()
    if paired_reads:
        output_pair_writer.close()

    return output_file_names, paired_reads

#################################
#  Aligner
#################################


def align_reads_star(sample_name, file_names, alignment_output_dir):
    # If paired read flag is required
    # paired_read = True if len(file_names) == 2 else False

    print("Aligning reads...")
    aligner_args = "{app_folder}/STAR/STAR --runThreadN 4 {aligner_extra_args} --genomeDir {index_folder} " \
                   "--readFilesIn {fastq_file_names} --outFileNamePrefix {output_folder} --outSAMtype BAM Unsorted".\
        format(app_folder=APPLICATION_FOLDER,
               aligner_extra_args="" if parser_result.aligner_extra_args is None else parser_result.aligner_extra_args,
               index_folder=GENOME_REFERENCES_FOLDER + "/star_index",
               fastq_file_names=" ".join(file_names),
               output_folder=alignment_output_dir + "/")
    print("Command: " + aligner_args)
    aligner_process = Popen(shlex.split(aligner_args), stdout=PIPE, stderr=PIPE)
    aligner_out, aligner_error = aligner_process.communicate()

    if aligner_process.returncode != 0:
        raise ValueError("STAR failed to complete (Non-zero return code)!\n"
                         "STAR stdout: {std_out} \nSTAR stderr: {std_err}".format(std_out=aligner_out.decode("utf8"),
                                                                                  std_err=aligner_error.decode("utf8")))

    if aligner_error.decode("utf8").strip() != "" or not os.path.isfile(alignment_output_dir + "/Log.final.out"):
        raise ValueError("STAR failed to complete (No output file is found)!\n"
                         "STAR stdout: {std_out} \nSTAR stderr: {std_err}".format(std_out=aligner_out.decode("utf8"),
                                                                                  std_err=aligner_error.decode("utf8")))

    print('Completed reads alignment')

    bam_file_name_output = "Aligned.out.bam"

    return bam_file_name_output


def align_reads_hisat(sample_name, file_names, alignment_output_dir):
    # If paired read flag is required
    paired_read = True if len(file_names) == 2 else False

    print("Aligning reads...")
    if paired_read:
        fastq_file_args = "-1 {} -2 {}".format(*file_names)
    else:
        fastq_file_args = "-U {}".format(*file_names)

    aligner_args = "{app_folder}/hisat/hisat2 -p 4 --tmo {aligner_extra_args} -x {index_folder}/hisat2.index " \
                   "{fastq_file_names} -S {output_folder}/output.sam".\
        format(app_folder=APPLICATION_FOLDER,
               aligner_extra_args="" if parser_result.aligner_extra_args is None else parser_result.aligner_extra_args,
               index_folder=GENOME_REFERENCES_FOLDER + "/hisat_index",
               fastq_file_names=fastq_file_args,
               output_folder=alignment_output_dir)
    print("Command: " + aligner_args)
    aligner_process = Popen(shlex.split(aligner_args), stdout=PIPE, stderr=PIPE)
    aligner_out, aligner_error = aligner_process.communicate()

    if aligner_process.returncode != 0:
        raise ValueError("HISAT2 failed to complete (Non-zero return code)!\n"
                         "HISAT2 stdout: {std_out} \nHISAT2 stderr: {std_err}".format(std_out=aligner_out.decode("utf8"),
                                                                                      std_err=aligner_error.decode("utf8")))
    print('Completed reads alignment')

    samtools_args = "{app_folder}/samtools/samtools view -@ 4 -o {output_folder}/output.bam {output_folder}/output.sam". \
        format(app_folder=APPLICATION_FOLDER,
               output_folder=alignment_output_dir)
    print("Command: " + samtools_args)
    samtools_process = Popen(shlex.split(samtools_args), stdout=PIPE, stderr=PIPE)
    samtools_out, samtools_error = samtools_process.communicate()

    if samtools_process.returncode != 0:
        raise ValueError("Samtools failed to complete (Non-zero return code)!\n"
                         "Samtools stdout: {std_out} \nSamtools stderr: {std_err}".format(
            std_out=samtools_out.decode("utf8"), std_err=samtools_error.decode("utf8")))

    sam_file_name_output = "output.bam"

    return sam_file_name_output


def align_reads_subread(sample_name, file_names, alignment_output_dir):
    # If paired read flag is required
    paired_read = True if len(file_names) == 2 else False

    print("Aligning reads...")
    print("Aligning with subread")
    if paired_read:
        fastq_file_args = "-r {} -R {}".format(*file_names)
    else:
        fastq_file_args = "-r {}".format(*file_names)

    aligner_args = "{app_folder}/subread/subread-align -T 4 -t 0 --SAMoutput {aligner_extra_args} " \
                   "-i {index_folder}/genome {fastq_file_names} -o {output_folder}/output.bam".\
        format(app_folder=APPLICATION_FOLDER,
               aligner_extra_args="" if parser_result.aligner_extra_args is None else parser_result.aligner_extra_args,
               index_folder=GENOME_REFERENCES_FOLDER + "/subread_index",
               fastq_file_names=fastq_file_args,
               output_folder=alignment_output_dir)
    print("Command: " + aligner_args)
    aligner_process = Popen(shlex.split(aligner_args), stdout=PIPE, stderr=PIPE)
    aligner_out, aligner_error = aligner_process.communicate()

    if aligner_process.returncode != 0:
        raise ValueError("Subread failed to complete (Non-zero return code)!\n"
                         "Subread stdout: {std_out} \nSubread stderr: {std_err}".format(std_out=aligner_out.decode("utf8"),
                                                                                        std_err=aligner_error.decode("utf8")))
    print('Completed reads alignment')

    sam_file_name_output = "output.bam"

    return sam_file_name_output


#################################
#  Main functions
#################################


def alignment_step(keyval):
    # Input: file_name, file_content as key,val
    # Output: [sample_name, file_name] as [key,val]
    global parser_result

    prefix_regex = r"(.*_part[0-9]*)\."

    file_name, file_content = keyval
    prefix_match = re.findall(prefix_regex, file_name.rstrip("/").split("/")[-1])

    if len(prefix_match) != 1:
        raise ValueError("Filename can not be resolved (invalid, pattern mismatch): {}".format(file_name))

    prefix = prefix_match[0]
    sample_name = prefix.rsplit("_part", 1)[0]

    alignment_dir = TEMP_OUTPUT_FOLDER + "/alignment_" + prefix

    try:
        os.mkdir(alignment_dir)
    except:
        print('Alignment directory {} exist.'.format(alignment_dir))

    print("Recreating FASTQ file(s)")
    split_file_names, paired_reads = split_interleaved_file(prefix, file_content, alignment_dir)
    print("Recreating FASTQ file(s) complete. Files recreated: {}".format(",".join(split_file_names)))

    alignment_output_dir = alignment_dir + "/aligner_output"

    try:
        os.mkdir(alignment_output_dir)
    except:
        print('Alignment output directory {} exist.'.format(alignment_output_dir))

    if parser_result.aligner.lower() == "star":
        aligned_sam_output = align_reads_star(sample_name, split_file_names, alignment_output_dir)
    elif parser_result.aligner.lower() == "hisat" or parser_result.aligner.lower() == "hisat2":
        aligned_sam_output = align_reads_hisat(sample_name, split_file_names, alignment_output_dir)
    elif parser_result.aligner.lower() == "subread":
        aligned_sam_output = align_reads_subread(sample_name, split_file_names, alignment_output_dir)
    else:
        print("Aligner specified is not yet supported. Defaulting to STAR")
        aligned_sam_output = align_reads_star(sample_name, split_file_names, alignment_output_dir)

    aligned_output_filepath = "{}/{}".format(alignment_output_dir.rstrip("/"), aligned_sam_output)
    aligned_output_hdfs_filepath = "{}/{}".format(HDFS_TEMP_OUTPUT_FOLDER, prefix)

    subprocess.call(["hdfs", "dfs", "-rm", aligned_output_hdfs_filepath])
    subprocess.call(["hdfs", "dfs", "-put", aligned_output_filepath, aligned_output_hdfs_filepath])

    shutil.rmtree(alignment_dir, ignore_errors=True)
    return sample_name, [prefix]


def fuse_alignment(keyval):
    # Input: sample_name, [file_name,...] as key, val
    # Output: sample_name
    global parser_result

    key, file_lists = keyval
    fuse_alignment_dir = TEMP_OUTPUT_FOLDER.rstrip("/") + "/" + key

    ordered_file_lists = sorted([(f, int(f.rsplit("part", 1)[-1])) for f in file_lists], key=lambda x:x[-1])
    print(ordered_file_lists)

    try:
        os.mkdir(fuse_alignment_dir)
    except:
        print('Fuse alignment directory {} exist.'.format(fuse_alignment_dir))

    fuse_alignment_file = key + ".bam"

    previous_file_path = ""
    for index, file_name_pair in enumerate(ordered_file_lists):
        file_name, number = file_name_pair
        local_file_path = fuse_alignment_dir + "/" + file_name + ".bam"
        subprocess.call(["hdfs", "dfs", "-get", HDFS_TEMP_OUTPUT_FOLDER.rstrip("/") + "/" + file_name, local_file_path])

        if index != 0:
            new_merged_file_path = "{}/temp_{}.bam".format(fuse_alignment_dir, index)
            subprocess.call(["samtools", "cat", "-o", new_merged_file_path, previous_file_path, local_file_path])

            os.remove(previous_file_path)
            os.remove(local_file_path)
            previous_file_path = new_merged_file_path
        else:
            previous_file_path = local_file_path

        subprocess.call(["hdfs", "dfs", "-rm", HDFS_TEMP_OUTPUT_FOLDER.rstrip("/") + "/" + file_name])

    if parser_result.output_dir.startswith("s3://"):  # From S3
        s3_client = boto3.client('s3', region_name=parser_result.aws_region)
        print("uploading to S3")
        output_bucket, key_prefix = parser_result.output_dir.strip().strip("/")[5:].split("/", 1)
        s3_client.upload_file(previous_file_path, output_bucket, key_prefix + "/" + fuse_alignment_file)
    else:
        print("outputting to HDFS")
        subprocess.call(["hdfs", "dfs", "-mkdir", "-p", parser_result.output_dir.rstrip("/")])
        subprocess.call(["hdfs", "dfs", "-put", previous_file_path, parser_result.output_dir.rstrip("/") + "/" +
                         fuse_alignment_file])

    os.remove(previous_file_path)
    return key

if __name__ == "__main__":
    global parser_result

    parser = argparse.ArgumentParser(description='Spark-based RNA-seq Pipeline Alignment')
    parser.add_argument('--input', '-i', action="store", dest="input_dir", help="Input directory - HDFS or S3")
    parser.add_argument('--output', '-o', action="store", dest="output_dir", help="Output directory - HDFS or S3")
    parser.add_argument('--aligner_tools', '-at', action="store", dest="aligner", nargs='?',
                        help="Aligner to be used (STAR|HISAT2|Subread)", default="STAR")
    parser.add_argument('--aligner_extra_args', '-s', action="store", dest="aligner_extra_args", nargs='?',
                        help="Extra argument to be passed to alignment tool", default="")
    parser.add_argument('--region', '-r', action="store", dest="aws_region", help="AWS region")

    parser_result = parser.parse_args()

    split_num = 0

    conf = SparkConf().setAppName("Spark-based RNA-seq Pipeline Alignment")
    sc = SparkContext(conf=conf)

    if parser_result.input_dir.startswith("s3://"):  # From S3
        s3_client = boto3.client('s3', region_name=parser_result.aws_region)
        # Get number of input files
        s3_paginator = s3_client.get_paginator('list_objects')
        input_bucket, key_prefix = parser_result.input_dir[5:].strip().split("/", 1)

        input_file_num = 0

        for result in s3_paginator.paginate(Bucket=input_bucket, Prefix=key_prefix):
            for file in result.get("Contents"):
                input_file_num += 1

        if input_file_num == 0:
            raise ValueError("Input directory is invalid or empty!")

        split_num = input_file_num
    else:  # From HDFS
        hdfs_process = Popen(shlex.split("hdfs dfs -count {}".format(parser_result.input_dir)),
                             stdout=PIPE, stderr=PIPE)
        hdfs_out, hdfs_error = hdfs_process.communicate()

        if hdfs_error:
           raise ValueError("Input directory is invalid or empty!")

        dir_count, file_count, size, path = hdfs_out.strip().split()

        split_num = int(file_count)

    subprocess.call(["hdfs", "dfs", "-mkdir", "-p", HDFS_TEMP_OUTPUT_FOLDER])

    input_files = sc.wholeTextFiles(parser_result.input_dir, split_num)

    aligned_files = input_files.map(alignment_step)
    aligned_file_lists = aligned_files.reduceByKey(add)
    aligned_samples = aligned_file_lists.map(fuse_alignment)
    aligned_samples.collect()
