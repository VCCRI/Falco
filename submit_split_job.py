import configparser
import argparse
import tempfile
import boto3
import sys
import utility

from collections import OrderedDict

global job_configuration, cluster_id
job_configuration = "split_job.config"
cluster_id = ""


def check_configuration(config):
    if not utility.check_config(config, "job_config", ["name", "action_on_failure", "splitter_script",
                                                       "splitter_script_s3_location", "upload_splitter_script"]):
        return False

    if not utility.check_upload_config(config["job_config"], "upload_splitter_script", "splitter_script",
                                       "splitter_script_local_location", "splitter_script_s3_location"):
        return False

    if not utility.check_config(config, "script_arguments", ["manifest", "input_location", "output_location",
                                                             "report_location", "region"]):
        return False

    if not utility.check_s3_region(config["script_arguments"]["region"]):
        return False

    return True


def set_mapper_number(manifest_file):
    fastq_counts = 0

    if manifest_file.startswith("s3://"):
        s3 = boto3.resource("s3")

        bucket_name, key_prefix = manifest_file.strip().strip("/")[5:].split("/", 1)

        with tempfile.TemporaryDirectory() as tmpdirname:
            s3.meta.client.download_file(bucket_name, key_prefix, tmpdirname + "/manifest")

            for line in open(tmpdirname+"/manifest"):
                fastq_counts += 1
    else:
        for line in open(manifest_file):
            fastq_counts += 1

    return fastq_counts


def build_command(config):
    global cluster_id

    job_arguments = OrderedDict()
    job_arguments["JobFlowId"] = cluster_id

    step_arguments = OrderedDict()
    step_arguments['Name'] = config["job_config"]["name"]
    step_arguments["ActionOnFailure"] = config["job_config"]["action_on_failure"]

    hadoop_arguments = OrderedDict()
    hadoop_arguments["Jar"] = "command-runner.jar"

    command_args = ["hadoop-streaming",
                    "-D", 'mapreduce.job.name=FASTQ file split',
                    "-D", "mapreduce.task.timeout=86400000",
                    "-D", "mapreduce.map.speculative=false",
                    "-D", "mapreduce.reduce.speculative=false"]

    mapper_number = set_mapper_number(config["script_arguments"]["manifest"])
    command_args.append("-D")
    command_args.append("mapreduce.job.maps=" + str(mapper_number))

    command_args.append("-files")
    command_args.append(config["job_config"]["splitter_script_s3_location"].strip().strip("/") + "/" +
                        config["job_config"]["splitter_script"])

    command_args.append("-mapper")
    command_args.append('{} -i {} -o {} -r {}'.format(config["job_config"]["splitter_script"].strip(),
                                                        config["script_arguments"]["input_location"],
                                                        config["script_arguments"]["output_location"],
                                                        config["script_arguments"]["region"]))
    command_args.append("-reducer")
    command_args.append("org.apache.hadoop.mapred.lib.IdentityReducer")
    command_args.append("-numReduceTasks")
    command_args.append("1")

    command_args.append("-input")
    command_args.append(config["script_arguments"]["manifest"])
    command_args.append("-output")
    command_args.append(config["script_arguments"]["report_location"])

    hadoop_arguments['Args'] = command_args
    step_arguments["HadoopJarStep"] = hadoop_arguments
    job_arguments["Steps"] = [step_arguments]

    return job_arguments

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Job submission script for spark-based RNA-seq Pipeline')
    parser.add_argument('--config', '-c', action="store", dest="job_config", help="Job configuration file")
    parser.add_argument('--cluster-id', '-id', action="store", dest="cluster_id", help="Cluster ID for submission")
    parser.add_argument('--dry-run', '-d', action="store_true", dest="dry_run",
                        help="Produce the configurations for the job flow to be submitted")
    parser_result = parser.parse_args()

    if parser_result.job_config is not None and parser_result.job_config.strip() != "":
        job_configuration = parser_result.job_config.strip()

    config = configparser.ConfigParser()
    config.optionxform = str
    config.read(job_configuration)

    if parser_result.cluster_id is None or parser_result.cluster_id.strip() == "":
        cluster_id = utility.get_cluster_id(parser_result.dry_run)
    else:
        cluster_id = parser_result.cluster_id.strip()

    if cluster_id != "" and check_configuration(config):
        if config["job_config"].get("upload_splitter_script", "False") == "True":
            utility.upload_files_to_s3([(config["job_config"]["splitter_script"],
                                         config["job_config"]["splitter_script_local_location"],
                                         config["job_config"]["splitter_script_s3_location"])], parser_result.dry_run)

        job_argument = build_command(config)

        if not parser_result.dry_run:
            emr_client = boto3.client("emr")
            # warn user before removing any output
            out = config["script_arguments"]["output_location"]
            rep = config["script_arguments"]["report_location"]
            # find out which output dirs, if any, exist
            dirs_to_remove = utility.check_s3_path_exists([out, rep])
            if dirs_to_remove:
                response = input("About to remove any existing output directories." +
                                 "\n\n\t{}\n\nProceed? [y/n]: ".format(
                                     '\n\n\t'.join(dirs_to_remove)))
                while response not in ['y', 'n']:
                    response = input('Proceed? [y/n]: ')
                if response == 'n':
                    print("Program Terminated.  Modify config file to change " +
                          "output directories.")
                    sys.exit(0)
                # remove the output directories
                if not utility.remove_s3_files(dirs_to_remove):
                    print("Program terminated")
                    sys.exit(1)
            job_submission = emr_client.add_job_flow_steps(**job_argument)
            print("Submitted split job to cluster {}. Job id is {}".format(cluster_id, job_submission["StepIds"][0]))
        else:
            print(job_argument)
