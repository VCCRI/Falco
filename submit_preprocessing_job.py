import configparser
import argparse
import sys
import boto3

from collections import OrderedDict

# local imports
import utility

job_configuration = "preprocessing_job.config"
cluster_id = ""


def check_configuration(cfg):
    """
    checks the validity of configuration elements
    :param cfg: the configuration object (ConfigParser)
    :return: True or False
    """
    # general job configuration
    if not utility.check_config(cfg, "job_config", ["name", "action_on_failure", "mapper_memory"]):
        return False

    if not utility.check_upload_config(cfg["job_config"], 'upload_script', 'script',
                                       'script_local_location', 'script_s3_location'):
        return False

    # preprocessing script args
    if not utility.check_config(cfg, "script_arguments", ["manifest", "input_location",
                                                          "output_location", "report_location",
                                                          "region"]):
        return False

    if not utility.check_s3_region(cfg["script_arguments"]['region']):
        return False

    # user files
    # Note that the "files" item is optional
    if not utility.check_config(cfg, "user_script_config", []) and \
            not utility.check_upload_config(cfg["user_script_config"], 'upload_user_files', 'script',
                                            'user_files_local_location', 'user_files_s3_location', 'supporting_files'):
        return False

    return True


def set_mapper_number(clust_id, mem_per_mapper):
    """
    sets the number of mappers for the job
    number of mappers is minimum of:
        - total number of cpus in the cluster nodes
        - total cluster memory divided by min memory required per mapper
    :param clust_id: the cluster_id for this configuration/run
    :param mem_per_mapper: the specified amount of memory required by each mapper
    :return: an integer representing the number of mappers or -1 if invalid cluster_id
    """
    mem, cpu = utility.get_cluster_mem_cpu(clust_id)
    if mem < 0 or cpu < 0:
        # could be dry-run
        mappers = -1
    else:
        mappers = int(min(cpu, mem / mem_per_mapper))

    return mappers


def upload_files_to_s3(cfg, dry_run):
    """
    uploads files to aws s3 storage - and updates the configuration object with
    the details of the s3 files
    :param cfg: ConfigParser configuration object
    :param dry_run: flag to indicate if this is "dry run" or not
    :return: the configuration object
    """
    s3_upload_list = []

    section = "job_config"
    if cfg[section]["upload_script"] == "True":
        s3_upload_list.append((cfg[section]["script"],
                               cfg[section]["script_local_location"],
                               cfg[section]["script_s3_location"]))

    section = "user_script_config"
    if cfg[section]["upload_user_files"] == "True":
        # upload the compulsory user script
        s3_upload_list.append((cfg[section]["script"],
                               cfg[section]["user_files_local_location"],
                               cfg[section]["user_files_s3_location"]))

        # upload any optional user files
        if "supporting_files" in cfg[section]:
            for f in cfg[section]["supporting_files"].split(','):
                if f.strip() != "":
                    s3_upload_list.append((f.strip(), cfg[section]["user_files_local_location"],
                                           cfg[section]["user_files_s3_location"]))

    # call utility code to upload list of files to s3
    files = utility.upload_files_to_s3(s3_upload_list, dry_run)
    cfg["s3"] = {"files": files}

    return cfg


# build a string to store the files that will be included in the
# step command "-files" option
# This is a comma separated list of files (in our case s3 keys)
# The result is stored "in memory" config["step"]["files"]
def build_files_option(cfg):
    """
    builds the "-files" option that is passed as part of the job step
    - a comma separated string of files to be copied to each node when the step is executed
    - the config object is updated with details of all files and the subset of additional files
    :param cfg: the ConfigParser configuration object
    :return: the configuration object
    """
    # add preprocessing script
    section = "job_config"
    files = (cfg[section]["script_s3_location"].rstrip("/") + "/" +
             cfg[section]["script"])
    # add user script
    section = "user_script_config"
    prefix = "," + cfg[section]["user_files_s3_location"].rstrip("/") + "/"
    files += prefix + cfg[section]["script"]
    # add optional files
    optional_files = ""
    if "supporting_files" in cfg[section]:
        optional_files = " -a " + cfg[section]["supporting_files"]
        for f in [x.strip() for x in cfg[section]["supporting_files"].split(",")]:
            if f != "":
                files += prefix + f
    # store "in memory" to config
    cfg["step"] = {"files": files, "additional_files_option": optional_files}

    return cfg


def build_command(cfg):
    """
    Builds a dictionary of job arguments for the step command that is submitted to the AWS EMR cluster for this job
    :param cfg: the ConfigParser configuration object
    :return: an ordered dictionary of job arguments
    """
    global cluster_id

    job_arguments = OrderedDict()
    job_arguments["JobFlowId"] = cluster_id

    step_arguments = OrderedDict()
    step_arguments['Name'] = cfg["job_config"]["name"]
    step_arguments["ActionOnFailure"] = cfg["job_config"]["action_on_failure"]

    hadoop_arguments = OrderedDict()
    hadoop_arguments["Jar"] = "command-runner.jar"

    mapper_mbytes = int(cfg["job_config"]["mapper_memory"])
    command_args = ["hadoop-streaming",
                    "-D", 'mapreduce.job.name=Preprocessing',
                    "-D", "mapreduce.map.memory.mb=" + str(mapper_mbytes),
                    "-D", "mapreduce.job.reducer=0",
                    "-D", "mapreduce.task.timeout=86400000",
                    "-D", "mapreduce.map.speculative=false",
                    "-D", "mapreduce.reduce.speculative=false"]

    # number of mappers
    mapper_gbytes = float(mapper_mbytes) / 1024
    mapper_number = set_mapper_number(cluster_id, mapper_gbytes)
    if mapper_number < 0:
        mapper_number = 'None'
    else:
        mapper_number = str(mapper_number)
    command_args.append("-D")
    command_args.append("mapreduce.job.maps=" + mapper_number)

    command_args.append("-files")
    command_args.append(cfg["step"]["files"])

    command_args.append("-mapper")
    command_args.append(
        '{} -i {} -o {} -r {} -u {}{}'.format(cfg["job_config"]["script"].strip().split("/")[-1],
                                              cfg["script_arguments"]["input_location"],
                                              cfg["script_arguments"]["output_location"],
                                              cfg["script_arguments"]["region"],
                                              cfg["user_script_config"]["script"],
                                              cfg["step"]["additional_files_option"]))

    command_args.append("-input")
    command_args.append(cfg["script_arguments"]["manifest"])
    command_args.append("-output")
    command_args.append(cfg["script_arguments"]["report_location"])

    hadoop_arguments['Args'] = command_args
    step_arguments["HadoopJarStep"] = hadoop_arguments
    job_arguments["Steps"] = [step_arguments]

    return job_arguments


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Job submission script for spark-based RNA-seq Pipeline - Preprocessing')
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
        if (config["job_config"]["upload_script"] == "True" or
                config["user_script_config"]["upload_user_files"] == "True"):
            config = upload_files_to_s3(config, parser_result.dry_run)

        # build the "-files" step option string - which is the list of
        # files that need to be copied when executing the step
        config = build_files_option(config)

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
            print("Submitted preprocessing job to cluster {}. Job id is {}".format(cluster_id,
                                                                                   job_submission["StepIds"][0]))
        else:
            print(job_argument)
