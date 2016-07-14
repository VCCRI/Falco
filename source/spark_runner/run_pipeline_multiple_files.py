import argparse
import os
import shlex
import shutil
from subprocess import Popen, PIPE
from pyspark import SparkContext, SparkConf
import pandas as pd
import boto3

global parser_result

star_collected_metrics = ["number of input reads", "uniquely mapped reads number", "number of splices: total",
                          "number of splices: annotated (sjdb)", "number of splices: gt/ag", "number of splices: gc/ag",
                          "number of splices: at/ac", "number of splices: non-canonical",
                          "number of reads mapped to multiple loci", "number of reads mapped to too many loci"]

picard_collected_metrics = ['pf_bases', 'pf_aligned_bases', 'ribosomal_bases', 'coding_bases', 'utr_bases',
                            'intronic_bases', 'intergenic_bases', 'ignored_reads', 'correct_strand_reads',
                            'incorrect_strand_reads']


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


def align_reads(sample_name, file_names, alignment_output_dir):
    # If paired read flag is required
    # paired_read = True if len(file_names) == 2 else False

    print("Aligning reads...")
    aligner_args = "/mnt/app/STAR/STAR --runThreadN 4 {} --genomeDir /mnt/ref/star_ref --readFilesIn {} " \
                "--outFileNamePrefix {}".format("" if parser_result.star_extra_args is None else
                                                parser_result.star_extra_args,
                                                " ".join(file_names), alignment_output_dir + "/")
    print("Command: " + aligner_args)
    aligner_process = Popen(shlex.split(aligner_args), stdout=PIPE, stderr=PIPE)
    aligner_out, aligner_error = aligner_process.communicate()

    if aligner_error.strip() != "" or not os.path.isfile(alignment_output_dir + "/Log.final.out"):
        raise ValueError("STAR failed to complete (No output file is found)!\n"
                         "STAR stdout: {} \nSTAR stderr: {}".format(aligner_out, aligner_error))
    print('Completed reads alignment')

    aligner_qc_output = []
    with open(alignment_output_dir + "/Log.final.out") as star_qc:
        for line in star_qc:
            parts = line.strip().split("\t")
            if len(parts) < 2:
                continue
            aligner_metric_name, aligner_metric_value = parts[0].strip("| "), parts[1].strip()
            if aligner_metric_name.lower() in star_collected_metrics:
                aligner_qc_output.append((sample_name + "\t" + "QC_STAR_" + aligner_metric_name.replace(" ", "_"),
                                          int(aligner_metric_value)))

    sam_file_name_output = "Aligned.out.sam"

    return sam_file_name_output, aligner_qc_output


def count_reads(sample_name, aligned_sam_filepath, paired_reads, counter_output_dir):
    print("Counting reads...")
    counter_args = "/mnt/app/subread/featureCounts {} -T 4 {} -a /mnt/ref/genome_ref/{} -o {}/counts.txt {}".format(
        "-p" if paired_reads else "", "" if parser_result.counter_extra_args is None else
        parser_result.counter_extra_args, parser_result.annotation_file,
        counter_output_dir, aligned_sam_filepath)
    print("Command: " + counter_args)
    counter_process = Popen(shlex.split(counter_args), stdout=PIPE, stderr=PIPE)
    counter_out, counter_error = counter_process.communicate()

    if "[Errno" in counter_error.strip() or "error" in counter_error.strip().lower():
        raise ValueError("featureCount failed to complete! (Error)\nCounter stdout: {} \nCounter stderr: {}".
                         format(counter_out, counter_error))

    if not os.path.isfile(counter_output_dir + "/counts.txt"):
        raise ValueError("featureCount failed to complete! (No output file is found)\n"
                         "Counter stdout: {} \nCounter stderr: {}".format(counter_out, counter_error))

    counter_output = []
    with open(counter_output_dir + "/counts.txt") as f:
        for index, line in enumerate(f):
            if index < 2:  # Command summary and header
                continue

            line = line.strip().split()
            if len(line) == 0:
                print(line)
            gene, count = line[0], line[-1]
            counter_output.append((sample_name + "\t" + gene, int(count)))

    counter_qc_output = []
    with open(counter_output_dir + "/counts.txt.summary") as counter_qc:
        for line in counter_qc:
            parts = line.strip().split("\t")
            if parts[0] != "Status" and len(parts) == 2:
                counter_qc_output.append((sample_name + "\t" + "QC_counter_" + parts[0].lower(), int(parts[1])))

    return counter_output, counter_qc_output


def run_picard(sample_name, aligned_sam_filepath, picard_output_dir):
    print("Getting alignment metrics...")
    picard_args = "java8 -jar /mnt/app/picard-tools/picard.jar CollectRnaSeqMetrics I={} O={}/output.RNA_Metrics " \
                  "REF_FLAT=/mnt/ref/genome_ref/refFlat.txt STRAND={} {}". \
        format(aligned_sam_filepath, picard_output_dir, parser_result.strand_specificity,
               parser_result.picard_extra_args)
    print("Command: " + picard_args)
    picard_process = Popen(shlex.split(picard_args), stdout=PIPE, stderr=PIPE)
    picard_out, picard_error = picard_process.communicate()

    if not os.path.isfile(picard_output_dir + "/output.RNA_Metrics"):
        raise ValueError("Picard tools failed to complete (No output file is found)!\n"
                         "Picard tools stdout: {} \nPicard tools stderr: {}".format(picard_out, picard_error))

    picard_qc_output = []
    with open(picard_output_dir + "/output.RNA_Metrics") as picard_qc:
        picard_lines = picard_qc.readlines()

        index = 0
        while index < len(picard_lines):
            current_line = picard_lines[index].strip()

            if current_line.startswith("##") and current_line[2:].strip().startswith("METRICS CLASS"):
                picard_metric_header = picard_lines[index + 1].strip().lower().split("\t")
                picard_metric_value = picard_lines[index + 2].strip().split("\t")

                metrics = dict(zip(picard_metric_header, picard_metric_value))
                for metric in picard_collected_metrics:
                    if metrics[metric] != "":
                        picard_qc_output.append((sample_name + "\t" + "QC_picard_" + metric, int(metrics[metric])))
                index += 2
            index += 1

    return picard_qc_output


def sum_gene_counts(cumulative_count, current_count):
    return cumulative_count + current_count


def set_gene_id_as_key(keyval):
    # Input: file_name\tgene, count as key,val
    # Output: file_name, (gene,count) as key,val
    key, val = keyval
    file_group, gene_id = key.split("\t")

    if gene_id == "QC_STAR_total_reads":
        print(keyval)

    return gene_id, [(file_group, val)]


def merge_count_by_gene_id(file_count_one, file_count_two):
    return file_count_one + file_count_two


def process_count_by_gene_id(keyval):
    gene_id, counts = keyval

    return pd.DataFrame({k: v for k, v in counts}, index=[gene_id])


def combine_gene_counts(df_one, df_two):
    return df_one.append(df_two)

#################################
#  Main functions
#################################


def alignment_count_step(keyval):
    # Input: file_name, file_content as key,val
    # Output: [sample_name\tgene, count] as [key,val]
    global parser_result, star_collected_metrics, picard_collected_metrics

    file_name, file_content = keyval
    prefix = file_name.rstrip("/").split("/")[-1].split(".")[0]
    sample_name = prefix.rsplit("_part", 1)[0]

    alignment_dir = "/mnt/output/alignment_" + prefix

    try:
        os.mkdir(alignment_dir)
    except:
        print('Alignment directory {} exist.'.format(alignment_dir))

    print("Recreating FASTQ file(s)")
    split_file_names, paired_reads = split_interleaved_file(prefix, file_content, alignment_dir)
    print("Recreating FASTQ file(s) complete. Files recreated: {}".format(",".join(split_file_names)))

    alignment_output_dir = alignment_dir + "/star_output"

    try:
        os.mkdir(alignment_output_dir)
    except:
        print('Alignment output directory {} exist.'.format(alignment_output_dir))

    aligned_sam_output, aligner_qc_output = align_reads(sample_name, split_file_names, alignment_output_dir)

    aligned_sam_filepath = "{}/{}".format(alignment_output_dir.rstrip("/"), aligned_sam_output)

    counter_output, counter_qc_output = count_reads(sample_name, aligned_sam_filepath, paired_reads,
                                                    alignment_output_dir)

    counter_output.extend(aligner_qc_output)
    counter_output.extend(counter_qc_output)

    if parser_result.run_picard:
        picard_qc_output = run_picard(sample_name, aligned_sam_filepath, alignment_output_dir)
        counter_output.extend(picard_qc_output)

    shutil.rmtree(alignment_dir, ignore_errors=True)
    return counter_output

if __name__ == "__main__":
    global parser_result

    parser = argparse.ArgumentParser(description='Spark-based RNA-seq Pipeline')
    parser.add_argument('--input', '-i', action="store", dest="input_dir", help="Input directory - HDFS or S3")
    parser.add_argument('--output', '-o', action="store", dest="output_dir", help="Output directory - HDFS or S3")
    parser.add_argument('--annotation', '-a', action="store", dest="annotation_file",
                        help="Name of annotation file to be used")
    parser.add_argument('--strand_specificity', '-ss', action="store", dest="strand_specificity", nargs='?',
                        help="Strand specificity: NONE|FIRST_READ_TRANSCRIPTION_STRAND|SECOND_READ_TRANSCRIPTION_STRAND"
                        , default="NONE")
    parser.add_argument('--run_picard', '-rp', action="store_true", dest="run_picard", help="Run picard")
    parser.add_argument('--star_extra_args', '-s', action="store", dest="star_extra_args", nargs='?',
                        help="Extra argument to be passed to STAR", default="")
    parser.add_argument('--counter_extra_args', '-c', action="store", dest="counter_extra_args", nargs='?',
                        help="Extra argument to be passed to featureCount", default="")
    parser.add_argument('--picard_extra_args', '-p', action="store", dest="picard_extra_args", nargs='?',
                        help="Extra argument to be passed to picard tools", default="")
    parser.add_argument('--region', '-r', action="store", dest="aws_region", help="AWS region")

    parser_result = parser.parse_args()

    split_num = 0

    conf = SparkConf().setAppName("Spark-based RNA-seq Pipeline Multifile")
    sc = SparkContext(conf=conf)

    s3_client = boto3.client('s3', region_name=parser_result.aws_region)

    if parser_result.input_dir.startswith("s3://"):  # From S3

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

        split_num = file_count

    input_files = sc.wholeTextFiles(parser_result.input_dir, split_num)

    count_output = input_files.flatMap(alignment_count_step).reduceByKey(sum_gene_counts)
    count_by_gene = count_output.map(set_gene_id_as_key).reduceByKey(merge_count_by_gene_id) \
        .map(process_count_by_gene_id)
    count_summary = count_by_gene.reduce(combine_gene_counts)

    count_qc_index = [f.startswith("QC_") for f in count_summary.index]
    count_only_index = [not x for x in count_qc_index]

    count_only_summary = count_summary[count_only_index]
    count_qc_summary = count_summary[count_qc_index]

    # If normalisation is required
    # count_summary = count_summary.apply(lambda x: x / np.sum(x) * 1000000)
    expressions_file = 'samples_expression.csv'
    qc_report_file = 'samples_qc_report.csv'
    count_only_summary = count_only_summary.sort_index()
    count_only_summary.to_csv(expressions_file)

    count_qc_summary = count_qc_summary.sort_index()
    count_qc_summary.to_csv(qc_report_file)

    output_bucket, key_prefix = parser_result.output_dir.strip().strip("/")[5:].split("/", 1)
    s3_client.upload_file(expressions_file, output_bucket, key_prefix + "/" + expressions_file)
    s3_client.upload_file(qc_report_file, output_bucket, key_prefix + "/" + qc_report_file)

    os.remove(expressions_file)
    os.remove(qc_report_file)
