#! /usr/bin/python3

import io
import gzip
import subprocess
import os
import boto3
import argparse
import sys
import shutil

global record_separator, pair_separator, parser_result, max_split_size
max_split_size = 256 # 256 MB
record_separator = "\t"
pair_separator = "\t"

TEMP_OUTPUT_FOLDER = "/mnt/output"


def split_reads(file_list):
    global parser_result

    if len(file_list) == 2:
        file_one, file_two = file_list
    else:
        file_one, file_two = file_list[0], None

    gzipped_file = False
    if file_one.endswith(".gz"):
        gzipped_file = True
        output_prefix, suffix_extension = file_one[:-3].rsplit(".", 1)
    else:
        output_prefix, suffix_extension = file_one.rsplit(".", 1)

    output_dir = TEMP_OUTPUT_FOLDER + "/processing_" + output_prefix

    try:
        os.mkdir(output_dir)
    except:
        shutil.rmtree(output_dir, ignore_errors=True)
        os.mkdir(output_dir)
        print('Output directory {} exist.'.format(output_dir), file=sys.stderr)

    # Download the file from s3
    if parser_result.input_dir.startswith("s3://"):
        s3_bucket, key_prefix = parser_result.input_dir[5:].strip("/").split('/', 1)

    split_file_names = []
    for file_name in file_list:

        if parser_result.input_dir.startswith("s3://"):
            s3_client = boto3.client("s3", region_name=parser_result.s3_region)
            try:
                s3_client.download_file(s3_bucket, key_prefix + "/" + file_name, output_dir + "/" + file_name)
            except:
                raise ValueError(s3_bucket + ":" + key_prefix + "/" + file_name, output_dir + "/" + file_name)
        else:
            try:
                subprocess.call(["hdfs", "dfs", "-get", parser_result.input_dir.rstrip("/") + "/" + file_name,
                                 output_dir + "/" + file_name])
            except:
                raise ValueError("Unable to retrieve file from HDFS: " + key_prefix + "/" + file_name)

        split_file_names.append(output_dir + "/" + file_name)

    file_part = 0
    output_name = "{}/{}_part{}.{}".format(output_dir, output_prefix, str(file_part), suffix_extension)
    output_name_gzip = output_name + ".gz"

    if gzipped_file:
        file_one_reader = io.TextIOWrapper(gzip.open(split_file_names[0]))
    else:
        file_one_reader = open(split_file_names[0])

    if file_two:
        if gzipped_file:
            file_two_reader = io.TextIOWrapper(gzip.open(split_file_names[1]))
        else:
            file_two_reader = open(split_file_names[1])

    output_writer = open(output_name, 'w')

    single_record = []
    paired_record = []
    index = 0

    for read_line in file_one_reader:
        # After collecting one FASTQ record, write it to file
        if index % 4 == 0 and index != 0:
            if file_two:
                record_output = record_separator.join(single_record) + pair_separator + \
                                record_separator.join(paired_record) + "\n"
            else:
                record_output = record_separator.join(single_record) + "\n"

            single_record = []
            paired_record = []
            output_writer.write(record_output)

            if output_writer.tell() > (max_split_size * 1024 * 1024):

                output_writer.close()
                subprocess.call(["gzip", "-f", "--fast", output_name])

                upload_split(output_name_gzip)

                # If there is still more record to process, open a new file to write to
                if file_one_reader.buffer.peek(10):
                    file_part += 1
                    output_name = "{}/{}_part{}.{}".format(output_dir, output_prefix, str(file_part), suffix_extension)
                    output_name_gzip = output_name + ".gz"

                    output_writer = open(output_name, 'w')

        single_record.append(read_line.strip())
        if file_two:
            paired_record.append(file_two_reader.readline().strip())

        index += 1

    # Close the file if it's not yet closed and write the last line!
    if not output_writer.closed:
        if file_two:
            record_output = record_separator.join(single_record) + pair_separator + \
                            record_separator.join(paired_record) + "\n"
        else:
            record_output = record_separator.join(single_record) + "\n"

        output_writer.write(record_output)
        output_writer.close()
        subprocess.call(["gzip", "-f", "--fast", output_name])

        upload_split(output_name_gzip)

    shutil.rmtree(output_dir, ignore_errors=True)
    return "Success"


def upload_split(file_name):
    global parser_result

    base_file_name = file_name.split("/")[-1]

    if parser_result.output_dir.startswith("s3://"):
        bucket_name, folder_name = parser_result.output_dir[5:].split("/", 1)
        key_name = "{}/{}".format(folder_name.rstrip("/"), base_file_name)

        s3 = boto3.resource("s3", region_name=parser_result.s3_region)
        s3.Bucket(bucket_name).upload_file(file_name, key_name)

        os.remove(file_name)

    else:
        subprocess.call(["hdfs", "dfs", "-mkdir", "-p", parser_result.output_dir])
        subprocess.call(["hdfs", "dfs", "-put", file_name, parser_result.output_dir])

    print(base_file_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='File splitter for spark-based RNA-seq Pipeline')
    parser.add_argument('--input', '-i', action="store", dest="input_dir",
                        help="Input location (S3 bucket)")
    parser.add_argument('--output', '-o', action="store", dest="output_dir", help="output location", required=True)
    parser.add_argument('--region', '-r', action="store", dest="s3_region", help="Region for S3 bucket",
                        nargs="?", default="us-east-1")
    parser_result = parser.parse_args()

    parser_result.output_dir = parser_result.output_dir.strip().rstrip("/")

    for line in sys.stdin:
        file_list = line.strip().split()
        split_reads(file_list)
