import argparse
import datetime
import os
import sys
import shlex
import shutil
from subprocess import Popen, PIPE
from operator import add
from pyspark import SparkContext, SparkConf
import pyspark.serializers
import subprocess
import boto3
import re
import pysam

global parser_result

if sys.version > "3.4":
    pyspark.serializers.protocol = 4

APPLICATION_FOLDER = "/app"
GENOME_REFERENCES_FOLDER = "/mnt/ref"
TEMP_OUTPUT_FOLDER = "/mnt/output"

BIN_BP_SIZE = 320000
BIN_BP_OFFSET = 64000 # Note: offset must never be greater than (BIN_BP_SIZE/2)+1!
BIN_BP_SIZE_W_OFFSET = BIN_BP_SIZE-BIN_BP_OFFSET
MINIMUM_READS_IN_BIN = 50
CHROMOSOME_PATTERN = r"^chr[0-9XY]+"


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


def align_reads_star(file_names, alignment_output_dir):
    # If paired read flag is required
    # paired_read = True if len(file_names) == 2 else False

    print("Aligning reads...")
    aligner_args = "{app_folder}/STAR/STAR --runThreadN 4 --outSAMstrandField intronMotif {aligner_extra_args} " \
                   "--genomeDir {index_folder} --readFilesIn {fastq_file_names} --outFileNamePrefix {output_folder}". \
        format(app_folder=APPLICATION_FOLDER,
               aligner_extra_args="" if parser_result.aligner_extra_args is None else parser_result.aligner_extra_args,
               index_folder=GENOME_REFERENCES_FOLDER + "/star_index",
               fastq_file_names=" ".join(file_names),
               output_folder=alignment_output_dir + "/")
    print("Command: " + aligner_args)
    aligner_process = Popen(shlex.split(aligner_args), stdout=PIPE, stderr=PIPE)
    aligner_out, aligner_error = aligner_process.communicate()

    if aligner_process.returncode != 0:
        raise ValueError(
            "STAR failed to complete (Non-zero return code)!\n"
            "STAR stdout: {std_out} \n"
            "STAR stderr: {std_err}".
            format(std_out=aligner_out.decode("utf8"), std_err=aligner_error.decode("utf8"))
        )

    if aligner_error.decode("utf8").strip() != "" or not os.path.isfile(alignment_output_dir + "/Log.final.out"):
        raise ValueError("STAR failed to complete (No output file is found)!\n"
                         "STAR stdout: {std_out} \n"
                         "STAR stderr: {std_err}".
                         format(std_out=aligner_out.decode("utf8"), std_err=aligner_error.decode("utf8")))

    print('Completed reads alignment')

    sam_file_name_output = "Aligned.out.sam"
    return sam_file_name_output


def align_reads_hisat(file_names, alignment_output_dir):
    # If paired read flag is required
    paired_read = True if len(file_names) == 2 else False

    print("Aligning reads...")
    if paired_read:
        fastq_file_args = "-1 {} -2 {}".format(*file_names)
    else:
        fastq_file_args = "-U {}".format(*file_names)

    aligner_args = "{app_folder}/hisat/hisat2 -p 4 --no-unal --no-mixed {aligner_extra_args} " \
                   "-x {index_folder}/hisat2.index {fastq_file_names} -S {output_folder}/output.sam". \
        format(app_folder=APPLICATION_FOLDER,
               aligner_extra_args="" if parser_result.aligner_extra_args is None else parser_result.aligner_extra_args,
               index_folder=GENOME_REFERENCES_FOLDER + "/hisat_index",
               fastq_file_names=fastq_file_args,
               output_folder=alignment_output_dir)
    print("Command: " + aligner_args)
    aligner_process = Popen(shlex.split(aligner_args), stdout=PIPE, stderr=PIPE)
    aligner_out, aligner_error = aligner_process.communicate()

    if aligner_process.returncode != 0:
        raise ValueError(
            "HISAT2 failed to complete (Non-zero return code)!\n"
            "HISAT2 stdout: {std_out} \n"
            "HISAT2 stderr: {std_err}".
            format(std_out=aligner_out.decode("utf8"), std_err=aligner_error.decode("utf8"))
        )
    print('Completed reads alignment')

    sam_file_name_output = "output.sam"
    return sam_file_name_output


def align_reads_subread(file_names, alignment_output_dir):
    # If paired read flag is required
    paired_read = True if len(file_names) == 2 else False

    print("Aligning reads...")
    if paired_read:
        fastq_file_args = "-r {} -R {}".format(*file_names)
    else:
        fastq_file_args = "-r {}".format(*file_names)

    aligner_args = "{app_folder}/subread/subread-align -T 4 -t 0 --SAMoutput -u {aligner_extra_args} " \
                   "-i {index_folder}/genome {fastq_file_names} -o {output_folder}/output.sam". \
        format(app_folder=APPLICATION_FOLDER,
               aligner_extra_args="" if parser_result.aligner_extra_args is None else parser_result.aligner_extra_args,
               index_folder=GENOME_REFERENCES_FOLDER + "/subread_index",
               fastq_file_names=fastq_file_args,
               output_folder=alignment_output_dir)
    print("Command: " + aligner_args)
    aligner_process = Popen(shlex.split(aligner_args), stdout=PIPE, stderr=PIPE)
    aligner_out, aligner_error = aligner_process.communicate()

    if aligner_process.returncode != 0:
        raise ValueError(
            "Subread failed to complete (Non-zero return code)!\n"
            "Subread stdout: {std_out} \n"
            "Subread stderr: {std_err}".format(
             std_out=aligner_out.decode("utf8"), std_err=aligner_error.decode("utf8"))
        )
    print('Completed reads alignment')

    sam_file_name_output = "output.sam"
    return sam_file_name_output


#################################
#  Binning
#################################


def bin_reads(aligned_output_filepath, paired_reads):
    binned_reads = []

    def process_read(read_one, read_two=None):
        if read_one.reference_id in chromosome_list_broadcast.value:
            read_start = read_one.reference_start
            read_end = read_one.reference_end
            read_one.query_sequence = ""
            read_one.query_qualities = ""

            if read_two:
                if read_two.reference_start < read_start:
                    read_start = read_two.reference_start

                if read_two.reference_end > read_end:
                    read_end = read_two.reference_end

                read_two.query_sequence = ""
                read_two.query_qualities = ""

            if parser_result.enable_tiling:
                start_bin_number = max(read_start-BIN_BP_OFFSET, 0) // BIN_BP_SIZE_W_OFFSET
                end_bin_number = read_end // BIN_BP_SIZE_W_OFFSET
            else:
                start_bin_number = read_start // BIN_BP_SIZE
                end_bin_number = read_end // BIN_BP_SIZE

            for bin_number in range(start_bin_number, end_bin_number+1):
                binned_reads.append(((read_one.reference_id, bin_number), [read_one.tostring() + "\n"]))
                if read_two:
                    binned_reads.append(((read_two.reference_id, bin_number), [read_two.tostring() + "\n"]))

    with pysam.AlignmentFile(aligned_output_filepath) as alignment_file:
        if paired_reads:
            previous_read = None
            for index, read in enumerate(alignment_file):
                if index % 2 == 1:
                    # For improperly paired reads (across multiple chromosome), we will treat them like single reads
                    if previous_read.reference_id != read.reference_id:
                        process_read(previous_read)
                        process_read(read)
                    else:
                        process_read(previous_read, read)
                else:
                    previous_read = read
        else:
            for read in alignment_file:
                process_read(read)

    return binned_reads


#################################
#  Assembly
#################################


def assemble_transcripts_stringtie(bin_id, aligned_output_filepath, assembler_output_dir, reference_gtf_filepath=None):
    print("Assembling reads...")
    assembler_command = "{app_folder}/stringtie/stringtie -p 8 -l {prefix} {assembler_extra_args} {reference_gtf} " \
                        "{aligned_file}".\
        format(app_folder=APPLICATION_FOLDER,
               prefix=bin_id,
               assembler_extra_args=parser_result.assembler_extra_args,
               reference_gtf="-G " + reference_gtf_filepath if reference_gtf_filepath else "",
               aligned_file=aligned_output_filepath)
    print("Command: " + assembler_command)
    assembler_process = Popen(shlex.split(assembler_command), stdout=PIPE, stderr=PIPE)
    assembler_out, assembler_error = assembler_process.communicate()

    if assembler_process.returncode != 0:
        raise ValueError('Stringtie failed to complete (non-zero return):\n'
                         'Stringtie stdout: {std_out}\n'
                         'Stringtie stderr: {std_err}'.
                         format(std_out=assembler_out.decode("utf8"), std_err=assembler_error.decode("utf8")))

    if assembler_error.decode("utf8").strip() != "":
        raise ValueError('Stringtie failed to complete (error):\n'
                         'Stringtie stdout: {std_out}\n'
                         'Stringtie stderr: {std_err}'.
                         format(std_out=assembler_out.decode("utf8"), std_err=assembler_error.decode("utf8")))

    return assembler_out.decode("utf8")


def assemble_transcripts_scallop(bin_id, aligned_output_filepath, assembler_output_dir):
    print("Assembling reads...")
    output_gtf = assembler_output_dir + "/output.gtf"
    assembler_command = "{app_folder}/scallop/scallop -i {sam_file} -o {out_gtf} {assembler_extra_args} --verbose 0".\
        format(app_folder=APPLICATION_FOLDER,
               sam_file=aligned_output_filepath,
               out_gtf=output_gtf,
               assembler_extra_args=parser_result.assembler_extra_args.strip())
    print("Command: " + assembler_command)
    assembler_process = Popen(shlex.split(assembler_command), stdout=PIPE, stderr=PIPE)
    assembler_out, assembler_error = assembler_process.communicate()

    if assembler_process.returncode != 0:
        raise ValueError('scallop failed to complete (non-zero return):\n'
                         'Scallop stdout: {std_out}\n'
                         'Scallop stderr: {std_err}'.
                         format(std_out=assembler_out.decode("utf8"), std_err=assembler_error.decode("utf8")))

    if not os.path.isfile(output_gtf):
        raise ValueError('Scallop failed to complete (no output file):\n'
                         'Scallop stdout: {std_out}\n'
                         'Scallop stderr: {std_err}'.
                         format(std_out=assembler_out.decode("utf8"), std_err=assembler_error.decode("utf8")))

    # We need to give each GTF entry a unique ID for the final merging using the region
    annotation_output = ""
    with open(output_gtf) as gtf:
        for line in gtf:
            if not line.startswith("#"):
                line = line.replace('"gene', '"{}'.format(bin_id))
            annotation_output += line

    return annotation_output


#################################
#  Augmenting Annotation
#################################


def merge_reference_annotation(assembled_transcript):
    assembled_transcript_path = "all_transcripts.gtf"
    merged_transcript_path = "reference.gtf"

    # Create a GTF file to pass to StringTie.
    with open(assembled_transcript_path, 'w') as gtf_file:
        gtf_file.writelines(assembled_transcript)
    assembled_transcript.clear()

    # Merge the reference annotation and transcripts from all bins.
    merge_command = "{app_folder}/stringtie/stringtie -p 32 {assembler_extra_args} --merge {assembled_transcript} " \
                    "-G {genome_ref_folder}/{annotation_file} -o {merged_transcript}".\
        format(app_folder=APPLICATION_FOLDER,
               assembler_extra_args=parser_result.assembler_merge_extra_args,
               assembled_transcript=assembled_transcript_path,
               genome_ref_folder=GENOME_REFERENCES_FOLDER + "/genome_ref",
               annotation_file=parser_result.annotation_file,
               merged_transcript=merged_transcript_path)
    print("Command: " + merge_command)
    merge_process = Popen(shlex.split(merge_command), stdout=PIPE, stderr=PIPE)
    merge_out, merge_error = merge_process.communicate()

    if merge_process.returncode != 0:
        raise ValueError('Stringtie (merge) failed to complete (non-zero return):\n'
                         'Stringtie stdout: {std_out}\n'
                         'Stringtie stderr: {std_err}\n'.
                         format(std_out=merge_out.decode("utf8"), std_err=merge_error.decode("utf8")))

    if merge_error.decode("utf8").strip() != "" or not os.path.isfile(merged_transcript_path):
        raise ValueError('Stringtie (merge) failed to complete (error):\n'
                         'Stringtie stdout: {std_out}\n'
                         'Stringtie stderr: {std_err}\n'.
                         format(std_out=merge_out.decode("utf8"), std_err=merge_error.decode("utf8")))

    return assembled_transcript_path, merged_transcript_path


def extract_reference_gtf(chromosome_name, assembler_output_dir):
    gtf_reference_extracted_path = assembler_output_dir + "/" + chromosome_name + "_annotation.gtf"

    gtf_extract_command = """awk '$1 == "{chr_name}"' {genome_ref_folder}/{annotation_file}""".\
        format(chr_name=chromosome_name,
               genome_ref_folder=GENOME_REFERENCES_FOLDER + "/genome_ref",
               annotation_file=parser_result.annotation_file)
    print("Command: " + gtf_extract_command)

    with open(gtf_reference_extracted_path, "w") as output_bam:
        gtf_extract_process = Popen(shlex.split(gtf_extract_command), stdout=output_bam, stderr=PIPE)
        gtf_extract_out, gtf_extract_error = gtf_extract_process.communicate()

    if gtf_extract_process.returncode != 0 or gtf_extract_error.decode("utf8").strip() != "":
        print("Extraction failed! Using full annotation file instead.\n"
              "Stdout: {std_out}\n"
              "Stderr: {std_err}".format(std_out=gtf_extract_out.decode("utf8"),
                                         std_err=gtf_extract_error.decode("utf8")))

        gtf_reference_extracted_path = "{genome_ref_folder}/{annotation_file}". \
            format(genome_ref_folder=GENOME_REFERENCES_FOLDER + "/genome_ref",
                   annotation_file=parser_result.annotation_file)

    # No reference data exist for the given chromosome. Will use de-novo argument instead.
    if os.stat(gtf_reference_extracted_path).st_size == 0:
        gtf_reference_extracted_path = None

    return gtf_reference_extracted_path


def analyse_transcripts(merged_transcript_path, analysis_output_dir):
    """
    Analyses the accuracy and precision of a generated GTF, with
    respect to a reference genome. Uses GFFCompare.
    :param merged_transcript_path: updated GTF
    :param analysis_output_dir: output directory
    :return: file path
    """
    # Merge the reference annotation and transcripts from all bins.
    gffcompare_command = "{app_folder}/gffcompare/gffcompare -T -r {genome_ref_folder}/{annotation_file} " \
                         "-o {analysis_output_dir}/gffcomp {merged_transcript}".\
        format(app_folder=APPLICATION_FOLDER,
               genome_ref_folder=GENOME_REFERENCES_FOLDER + "/genome_ref",
               annotation_file=parser_result.annotation_file,
               analysis_output_dir=analysis_output_dir,
               merged_transcript=merged_transcript_path)
    print("Command: " + gffcompare_command)
    gffcompare_process = Popen(shlex.split(gffcompare_command), stdout=PIPE, stderr=PIPE)
    gffcompare_out, gffcompare_error = gffcompare_process.communicate()

    if gffcompare_process.returncode != 0:
        raise ValueError('GFFcompare failed to complete (non-zero return):\n'
                         'GFFcompare stdout: {std_out}\n'
                         'GFFcompare stderr: {std_err}\n'.
                         format(std_out=gffcompare_out.decode("utf8"),std_err=gffcompare_error.decode("utf8")))

    gffcompare_stats_output = "gffcomp.stats"
    if gffcompare_error.decode("utf8").strip() != "" or not os.path.isfile(analysis_output_dir + "/" +
                                                                           gffcompare_stats_output):
        raise ValueError('GFFcompare failed to complete (error):\n'
                         'GFFcompare stdout: {std_out}\n'
                         'GFFcompare stderr: {std_err}\n'.
                         format(std_out=gffcompare_out.decode("utf8"), std_err=gffcompare_error.decode("utf8")))

    return gffcompare_stats_output

#################################
#  Main functions
#################################


def create_sam_header():
    alignment_dir = TEMP_OUTPUT_FOLDER + "/sam_header"

    try:
        os.mkdir(alignment_dir)
    except:
        print('Alignment directory {} exist.'.format(alignment_dir))

    file_name = os.path.join(alignment_dir, "empty.sam")
    if os.path.isfile(file_name):
        os.remove(file_name)

    os.mknod(file_name)

    if parser_result.aligner.lower() == "star":
        aligned_sam_output = align_reads_star([file_name], alignment_dir)
    elif parser_result.aligner.lower() == "hisat" or parser_result.aligner.lower() == "hisat2":
        aligned_sam_output = align_reads_hisat([file_name], alignment_dir)
    elif parser_result.aligner.lower() == "subread":
        aligned_sam_output = align_reads_subread([file_name], alignment_dir)
    else:
        print("Aligner specified is not yet supported. Defaulting to STAR")
        aligned_sam_output = align_reads_star([file_name], alignment_dir)

    aligned_output_filepath = os.path.join(alignment_dir, aligned_sam_output)
    with pysam.AlignmentFile(aligned_output_filepath) as f:
        sam_header = str(f.header)

        chromosome_list = {}
        for index, reference_name in enumerate(f.header.references):
            if re.search(CHROMOSOME_PATTERN, reference_name) is not None:
                chromosome_list[index] = reference_name

    shutil.rmtree(alignment_dir, ignore_errors=True)
    return sam_header, chromosome_list


def alignment_bin_step(keyval):
    # Input: file_name, file_content as key,val
    # Output: [sample_name\tgene, count] as [key,val]
    global parser_result

    prefix_regex = r"(.*_part[0-9]*)\."

    file_name, file_content = keyval
    prefix_match = re.findall(prefix_regex, file_name.rstrip("/").split("/")[-1])

    if len(prefix_match) != 1:
        raise ValueError("Filename can not be resolved (invalid, pattern mismatch): {}".format(file_name))

    prefix = prefix_match[0]

    alignment_dir = TEMP_OUTPUT_FOLDER + "/alignment_" + prefix
    try:
        os.mkdir(alignment_dir)
    except:
        print('Alignment directory {} exist.'.format(alignment_dir))

    print('{0:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()) + ":" + "Recreating FASTQ file(s)")
    split_file_names, paired_reads = split_interleaved_file(prefix, file_content, alignment_dir)
    print('{0:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()) + ":" +
          "Recreating FASTQ file(s) complete. Files recreated: {}".format(",".join(split_file_names)))

    alignment_output_dir = alignment_dir + "/aligner_output"

    try:
        os.mkdir(alignment_output_dir)
    except:
        print('Alignment output directory {} exist.'.format(alignment_output_dir))

    if parser_result.aligner.lower() == "star":
        aligned_sam_output = align_reads_star(split_file_names, alignment_output_dir)
    elif parser_result.aligner.lower() == "hisat" or parser_result.aligner.lower() == "hisat2":
        aligned_sam_output = align_reads_hisat(split_file_names, alignment_output_dir)
    elif parser_result.aligner.lower() == "subread":
        aligned_sam_output = align_reads_subread(split_file_names, alignment_output_dir)
    else:
        print("Aligner specified is not yet supported. Defaulting to STAR")
        aligned_sam_output = align_reads_star(split_file_names, alignment_output_dir)

    aligned_output_filepath = os.path.join(alignment_output_dir, aligned_sam_output)

    print('{0:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()) + ":" + "Binning reads")
    binned_reads = bin_reads(aligned_output_filepath, paired_reads)
    print('{0:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()) + ":" + "Binning reads done")

    print('{0:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()) + ":" + "Alignment of reads for {} done.".format(prefix))
    shutil.rmtree(alignment_dir, ignore_errors=True)
    return binned_reads


def assemble_transcripts(keyval):
    """
    Applies Stringtie to formulate transcripts.
    :param region_reads:
    :return: (chromosome_region, gtf_output) tuple
    """
    bin_id, read_list = keyval
    chromosome_id, bin_number = bin_id
    chromosome_name = chromosome_list_broadcast.value[chromosome_id]

    output = []

    if len(read_list) >= MINIMUM_READS_IN_BIN:
        print('{0:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()) + ":" + "Starting transcript assembly for {}.".format(bin_id))
        sample_prefix = "{}_{}".format(chromosome_name, bin_number)
        assembler_output_dir = TEMP_OUTPUT_FOLDER + '/assembly_' + sample_prefix
        try:
            os.makedirs(assembler_output_dir)
        except:
            print("Assembler output directory already exists for %s." % sample_prefix)

        aligned_output_file = assembler_output_dir + '/output.sam'
        aligned_sorted_output_file = assembler_output_dir + '/output.sorted.bam'

        with open(aligned_output_file, 'w') as sam_outfile:
            sam_outfile.write(sam_header_broadcast.value.strip() + "\n")
            sam_outfile.writelines(read_list)

        read_list.clear()

        print('{0:%Y-%m-%d %H:%M:%S}'.format(
            datetime.datetime.now()) + ":" + "Sorting and converting sam file.".format(bin_id))
        pysam.sort("-@", '4', "-m", "1G", "-o", aligned_sorted_output_file, aligned_output_file)

        print('{0:%Y-%m-%d %H:%M:%S}'.format(
            datetime.datetime.now()) + ":" + "Running stringtie.".format(bin_id))

        if parser_result.assembler.lower() == "stringtie":
            # If we are performing genome-guided assembly, build a smaller GTF file.
            reference_gtf_filepath = None
            if parser_result.assembler_use_reference:
                reference_gtf_filepath = extract_reference_gtf(chromosome_name, assembler_output_dir)

            gtf_output = assemble_transcripts_stringtie(sample_prefix, aligned_sorted_output_file, assembler_output_dir,
                                                        reference_gtf_filepath)
        elif parser_result.assembler.lower() == "scallop":
            gtf_output = assemble_transcripts_scallop(sample_prefix, aligned_sorted_output_file, assembler_output_dir)

        shutil.rmtree(assembler_output_dir, ignore_errors=True)
        print('{0:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()) + ":" + "Assembly of transcript for {} done.".format(bin_id))
        output.append((bin_id, gtf_output.strip()+"\n"))

    return output


if __name__ == "__main__":
    global parser_result

    parser = argparse.ArgumentParser(description='Spark-based RNA-seq Pipeline')
    parser.add_argument('--input', '-i', action="store", dest="input_dir", help="Input directory - HDFS or S3")
    parser.add_argument('--output', '-o', action="store", dest="output_dir", help="Output directory - HDFS or S3")
    parser.add_argument('--annotation', '-a', action="store", dest="annotation_file",
                        help="Name of annotation file to be used")
    parser.add_argument('--enable-tiling', '-et', action="store_true", dest="enable_tiling",
                        help="Enable tiling of genome bins")
    parser.add_argument('--enable-analysis', '-ea', action="store_true", dest="enable_analysis",
                        help="Generate stats on updated GTF w.r.t. reference GTF")
    parser.add_argument('--aligner-tools', '-at', action="store", dest="aligner", nargs='?',
                        help="Aligner to be used (STAR|HISAT2|Subread)", default="STAR")
    parser.add_argument('--aligner-extra-args', '-s', action="store", dest="aligner_extra_args", nargs='?',
                        help="Extra argument to be passed to alignment tool", default="")
    parser.add_argument('--assembler-tools', '-as', action="store", dest="assembler", nargs='?',
                        help="Assembler tools to be used (StringTie|Scallop)", default="StringTie")
    parser.add_argument('--assembler-extra-args', '-ag', action="store", dest="assembler_extra_args", nargs='?',
                        help="Extra arguments to be passed to the assembler tool", default="")
    parser.add_argument('--assembler-use-reference', '-aur', action="store_true", dest="assembler_use_reference",
                        help="Use annotation in initial transcript assembly - only applicable to StringTie")
    parser.add_argument('--assembler-merge-extra-args', '-am', action="store", dest="assembler_merge_extra_args",
                        nargs='?', help="Extra arguments to be passed to the assembler merge tool", default="")
    parser.add_argument('--region', '-r', action="store", dest="aws_region", help="AWS region")

    parser_result = parser.parse_args()

    split_num = 0

    conf = SparkConf().setAppName("Spark-based RNA-seq Pipeline for Read Assembly")
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

    print('{0:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()) + ":" + "Create sam header.")
    sam_header, chromosome_list = create_sam_header()
    sam_header_broadcast = sc.broadcast(sam_header)
    chromosome_list_broadcast = sc.broadcast(chromosome_list)
    print('{0:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()) + ":" + "Sam header done.")

    input_files = sc.wholeTextFiles(parser_result.input_dir, split_num)
    read_binned = input_files.flatMap(alignment_bin_step).reduceByKey(add, numPartitions=split_num*2)

    binned_transcripts = read_binned.flatMap(assemble_transcripts).persist()
    binned_transcripts2 = binned_transcripts.sortByKey().values().collect()

    print('{0:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()) + ":" + "Starting to merge back to reference.")
    assembled_transcript, merged_transcript = merge_reference_annotation(binned_transcripts2)
    print('{0:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()) + ":" + "Merging back to reference completed.")

    if parser_result.input_dir.startswith("s3://"):  # From S3
        output_bucket, key_prefix = parser_result.output_dir.strip().strip("/")[5:].split("/", 1)
        s3_client.upload_file(assembled_transcript, output_bucket, key_prefix + "/" + assembled_transcript)
        s3_client.upload_file(merged_transcript, output_bucket, key_prefix + "/" + merged_transcript)
    else:
        subprocess.call(["hdfs", "dfs", "-mkdir", "-p", parser_result.output_dir.rstrip("/")])
        subprocess.call(["hdfs", "dfs", "-put", assembled_transcript, parser_result.output_dir.rstrip("/") + "/"
                         + assembled_transcript])
        subprocess.call(["hdfs", "dfs", "-put", merged_transcript, parser_result.output_dir.rstrip("/") + "/"
                         + merged_transcript])

    if parser_result.enable_analysis:
        print('{0:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()) + ":" + "Starting analysis of GTF.")
        analysis_output_dir = TEMP_OUTPUT_FOLDER + "/analysis"
        try:
            os.mkdir(analysis_output_dir)
        except:
            print('Analysis directory {} already exists.'.format(analysis_output_dir))

        analysis_result = analyse_transcripts(merged_transcript, analysis_output_dir)
        print('{0:%Y-%m-%d %H:%M:%S}'.format(datetime.datetime.now()) + ":" + "Analysis of GTF complete.")

        analysis_result_path = os.path.join(analysis_output_dir, analysis_result)
        # Rename the file to make it easier
        analysis_result_key = analysis_result.rsplit(".",1)[0] + ".txt"
        if parser_result.input_dir.startswith("s3://"):  # From S3
            s3_client.upload_file(analysis_result_path, output_bucket, key_prefix + "/" + analysis_result_key)
        else:
            subprocess.call(["hdfs", "dfs", "-put", analysis_result_path, parser_result.output_dir.rstrip("/") + "/"
                             + analysis_result_key])

        shutil.rmtree(analysis_output_dir, ignore_errors=True)

    os.remove(assembled_transcript)
    os.remove(merged_transcript)
