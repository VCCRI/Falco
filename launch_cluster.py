import configparser
import argparse
import json
import boto3
import utility
from collections import OrderedDict

global emr_configuration, emr_applications, cluster_config, optional_instance_config
emr_configuration = "emr_cluster.config"
emr_applications = ["Hadoop", "Spark", "Ganglia"]
cluster_config = "source/cluster_creator/cluster_config.json"
optional_instance_config = {"vpc_subnet": "Ec2SubnetId",
                            "master_security_group": "EmrManagedMasterSecurityGroup",
                            "slave_security_group": "EmrManagedSlaveSecurityGroup",
                            "service_access_security_group": "ServiceAccessSecurityGroup"}


def check_configuration(config):
    if not utility.check_config(config, "EMR", ["release_label", "software_installer_location",
                                                "genome_folder_location"]):
        return False

    if not utility.check_upload_config(config["EMR"], "upload_bootstrap_scripts", "bootstrap_scripts",
                                       "bootstrap_scripts_local_location", "bootstrap_scripts_s3_location"):
        return False

    if not utility.check_config(config, "EMR_nodes", ["key_name", "service_role", "instance_profile",
                                                      "master_instance_type", "master_instance_count",
                                                      "core_instance_type", "core_instance_count"]):
        return False

    release_version = config["EMR"]["release_label"].split("-")[-1].split(".")
    major_release_version = int(release_version[0])
    minor_release_version = int(release_version[1])
    if config["EMR_nodes"].get("custom_ami_id", "").strip() != "" \
            and not (major_release_version >= 5 and minor_release_version >= 7):
        print("\033[31mERROR: \033[0mCustom AMI can only be used with EMR release >= 5.7")
        return False

    return True


def build_command(config):
    global emr_applications, cluster_config

    emr_arguments = OrderedDict()

    # EMR configs
    if config["EMR"]["name"]:
        emr_arguments["Name"] = config["EMR"]["name"]

    if config["EMR"]["log_uri"]:
        emr_arguments["LogUri"] = config["EMR"]["log_uri"]

    emr_arguments["ReleaseLabel"] = config["EMR"]["release_label"]

    # Instances config
    emr_arguments["Instances"] = OrderedDict()

    instance_groups = []
    for node_type in ["master", "core"]:
        instance_specification = {}

        if int(config["EMR_nodes"][node_type + "_instance_count"]) == 0:
            continue

        instance_specification['Name'] = node_type + "_node"
        instance_specification['InstanceRole'] = node_type.upper()

        if config["EMR_nodes"].getboolean(node_type + "_instance_spot"):
            instance_specification['Market'] = "SPOT"
            instance_specification['BidPrice'] = config["EMR_nodes"][node_type + "_instance_bid_price"]
        else:
            instance_specification['Market'] = "ON_DEMAND"

        instance_specification['InstanceType'] = config["EMR_nodes"][node_type + "_instance_type"]
        instance_specification['InstanceCount'] = int(config["EMR_nodes"][node_type + "_instance_count"])

        instance_groups.append(instance_specification)

    emr_arguments["Instances"]["InstanceGroups"] = instance_groups

    if config["EMR_nodes"]["key_name"]:
        emr_arguments["Instances"]["Ec2KeyName"] = config["EMR_nodes"]["key_name"]

    emr_arguments["Instances"]["KeepJobFlowAliveWhenNoSteps"] = True

    for instance_config in optional_instance_config:
        if instance_config in config["EMR_nodes"] and config["EMR_nodes"][instance_config].strip() != "":
            emr_arguments["Instances"][optional_instance_config[instance_config]] = config["EMR_nodes"][instance_config]

    emr_arguments["Steps"] = [
        {
            "Name": "Setup Hadoop Debugging",
            "ActionOnFailure": "TERMINATE_CLUSTER",
            "HadoopJarStep": {
                "Jar": "/var/lib/aws/emr/step-runner/hadoop-jars/command-runner.jar",
                "MainClass": "state-pusher-script"
            }
        }
    ]

    if "bootstrap_scripts" in config["EMR"]:
        bootstrap_actions = []
        for bootstrap_script in config["EMR"]["bootstrap_scripts"].split(","):
            bootstrap_script = bootstrap_script.strip()

            bootstrap_action_args = []
            if bootstrap_script == "install_software.sh":
                bootstrap_action_args = [config["EMR"]["software_installer_location"]]
            elif bootstrap_script == "copy_reference.sh":
                bootstrap_action_args = [config["EMR"]["genome_folder_location"]]

            bootstrap_actions.append({
                "Name": bootstrap_script,
                "ScriptBootstrapAction": {
                    "Path": config["EMR"]["bootstrap_scripts_s3_location"].rstrip("/") + "/" + bootstrap_script,
                    "Args": bootstrap_action_args
                }
             })

        emr_arguments["BootstrapActions"] = bootstrap_actions

    emr_arguments["Applications"] = [{'Name': app} for app in emr_applications]
    emr_arguments["Configurations"] = json.loads(open(cluster_config).read()) if cluster_config else []
    emr_arguments["VisibleToAllUsers"] = True
    emr_arguments["JobFlowRole"] = config["EMR_nodes"]["instance_profile"]
    emr_arguments["ServiceRole"] = config["EMR_nodes"]["service_role"]

    if "custom_ami_id" in config["EMR_nodes"]:
        emr_arguments["CustomAmiId"] = config["EMR_nodes"]["custom_ami_id"]

        if "ebs_root_volume_size" in config["EMR_nodes"]:
            emr_arguments["EbsRootVolumeSize"] = config["EMR_nodes"]["ebs_root_volume_size"]

    return emr_arguments

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Cluster launcher for spark-based RNA-seq Pipeline')
    parser.add_argument('--config', '-c', action="store", dest="emr_config", help="EMR configuration file")
    parser.add_argument('--dry-run', '-d', action="store_true", dest="dry_run",
                        help="Produce the configurations for the cluster to be created")
    parser_result = parser.parse_args()

    if parser_result.emr_config and parser_result.emr_config.strip() != "":
        emr_configuration = parser_result.emr_config

    config = configparser.ConfigParser()
    config.read(emr_configuration)

    if check_configuration(config):
        if config["EMR"].get("upload_bootstrap_scripts", "False") == "True":
            utility.upload_files_to_s3(
                [(bootstrap_script.strip(), config["EMR"]["bootstrap_scripts_local_location"],
                 config["EMR"]["bootstrap_scripts_s3_location"])
                 for bootstrap_script in config["EMR"]["bootstrap_scripts"].split(",")],
                parser_result.dry_run)

        emr_argument = build_command(config)

        if not parser_result.dry_run:
            emr_client = boto3.client("emr")
            cluster_launch = emr_client.run_job_flow(**emr_argument)
            print("Cluster has been launched with ID", cluster_launch["JobFlowId"])
        else:
            print("\n".join(["{} = {}".format(*emr_arg) for emr_arg in list(emr_argument.items())]))
