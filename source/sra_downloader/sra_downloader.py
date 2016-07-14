#! /usr/bin/python3

import io
import gzip
import subprocess
import os
import boto3
import argparse
import sys
import shutil
from subprocess import Popen, PIPE

global record_separator, pair_separator, parser_result, max_split_size
max_split_size = 256 # 256 MB
record_separator = "\t"
pair_separator = "\t"


def download_SRA(sra_accession):
    global parser_result

    sra_dir = sra_accession[:6]
    sra_digits = "".join([c for c in sra_accession if c.isnumeric()])

    if len(sra_digits) > 9:
        raise ValueError("Invalid SRA accession ID. Maximum number of digit for accession is 9.")
    elif len(sra_digits) > 6:
        offset = len(sra_digits) - 6
        sra_dir += "/" + "0" * (3 - offset) + sra_digits[-offset:]

    era_url = "ftp://ftp.sra.ebi.ac.uk/vol1/fastq/{}/{}/*".format(sra_dir, sra_accession)

    output_dir = "/mnt/output/download_" + sra_accession

    try:
        os.mkdir(output_dir)
    except:
        shutil.rmtree(output_dir, ignore_errors=True)
        os.mkdir(output_dir)
        print('Output directory {} exist.'.format(output_dir), file=sys.stderr)

    wget_process = Popen(['wget', "-P", output_dir + "/", era_url], stdout=PIPE, stderr=PIPE)
    wget_output, wget_error = wget_process.communicate()

    file_list = [f for f in os.listdir(output_dir) if f.startswith(sra_accession)]

    if not (0 < len(file_list) < 3):
        raise ValueError("SRA download failed!\nstdout:{}\nstderr:{}".format(wget_output, wget_error))

    if parser_result.download_only:
        for file_name in file_list:
            upload_split(output_dir + "/" + file_name, print_output_name=False)
        print("\t".join(file_list))
    else:
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

        split_file_names = [(output_dir + "/" + file_name) for file_name in file_list]

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


def upload_split(file_name, print_output_name=True):
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

    if print_output_name:
        print(base_file_name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='SRA downloader for spark-based RNA-seq Pipeline')
    parser.add_argument('--output', '-o', action="store", dest="output_dir", help="output location",
                        required=True)
    parser.add_argument('--region', '-r', action="store", dest="s3_region", help="Region for S3 bucket",
                        nargs="?", default="us-east-1")
    parser.add_argument('--download_only', '-d', action="store_true", dest="download_only", help="Only download FASTQ")
    parser_result = parser.parse_args()

    parser_result.output_dir = parser_result.output_dir.strip().rstrip("/")

    for line in sys.stdin:
        download_SRA(line.strip())
