# Shared utility functions
import os
import boto3
from boto3.s3.transfer import S3Transfer
import botocore.exceptions
import tempfile

S3_REGIONS = "us-east-1,us-west-1,us-west-2,eu-west-1,eu-central-1,\
ap-northeast-1,ap-northeast-2,ap-southeast-1,ap-southeast-2,sa-east-1,sa-east-1"
INSTANCE_TYPES_FILE = 'utility/instance_types.txt'

RED = '\033[31m'
CYAN = '\033[36m'
GREEN = '\033[32m'
YELLOW = '\033[33m'
PURPLE = '\033[35m'
NC = '\033[0m'  # no colour
ERROR = RED + 'ERROR: ' + NC
INFO = YELLOW + 'INFO: ' + NC


def get_cluster_id(dry_run=False):
    """
    gets a cluster_id from currently running clusters
    :param dry_run: a boolean flag that creates a dummy cluster_id in the case of a dry-run
    :return: the cluster_id
    """
    cluster_id = ""
    emr_client = boto3.client("emr")
    check_valid_clusters = emr_client.list_clusters(ClusterStates=['BOOTSTRAPPING', 'RUNNING', 'WAITING'])
    valid_clusters = check_valid_clusters["Clusters"]

    if len(valid_clusters) == 1:
        cluster_id = valid_clusters[0]["Id"]
    elif len(valid_clusters) > 1:
        # raise ValueError("There is multiple active cluster! Please specify cluster ID using -id argument")
        print("Select a cluster from the list below:")
        for index, val_clus in enumerate(valid_clusters):
            print("[{}] Name={}, ID={}, status={}, launchtime={}".
                  format(index + 1, val_clus["Name"], val_clus["Id"],
                         val_clus["Status"]["State"], val_clus["Status"]["Timeline"]["CreationDateTime"]))
        selection = input("Type number in square bracket (or leave blank to cancel): ")

        while True:
            if selection.isdigit():
                if 0 < int(selection) <= len(valid_clusters):
                    cluster_id = valid_clusters[int(selection) - 1]["Id"]
                    break
                else:
                    selection = input("Invalid number! Type number in square bracket (or leave blank to cancel): ")
            else:
                if selection == "":
                    break
                else:
                    selection = input("Invalid input! Type number in square bracket (or leave blank to cancel): ")
    elif dry_run:
        cluster_id = "DRY_RUN"
    else:
        print("No valid cluster is running.  Please launch a cluster before attempting to submit a job. In the case "
              "that your cluster is starting, please wait until it has reached bootstrapping step.")

    return cluster_id


def check_s3_region(region):
    """
    checks that the specified AWS S3 region is valid
    :param region: the region string
    :return: True if region is valid; False otherwise
    """
    if region not in S3_REGIONS.split(','):
        print(ERROR + "Invalid s3 region provided." + NC)

        print("Valid s3 regions are: ")
        for reg in S3_REGIONS.split(','):
            print("\t" + reg)

        return False

    return True


def check_config(config, section, item_list):
    """
    checks values in the configuration are complete and valid
    :param config: a ConfigParser object containing the configuration information
    :param section: the name of the section
    :param item_list: a list of items (for this section)
    :return: True if configuration passes validation checks; False otherwise
    """
    if section not in config.sections():
        print(ERROR + "Missing " + YELLOW + section + NC + " section in configuration file!")
        return False

    for item in item_list:
        if item not in config[section]:
            print(ERROR + "Invalid entry in configuration file section: " + CYAN + "[" + section + "]" + NC)
            print("Item missing:\t" + YELLOW + item + NC)
            return False

    return True


def check_upload_config(section_config, flag, source, local_dir, s3_dest, optional_source=""):
    """
    checks that the supplied configuration is complete and valid
    :param section_config: a ConfigParser object containing the configuration information
    :param flag: a boolean flag that indicates if uploading to AWS S3 is required
    :param source: name of source file to upload
    :param local_dir: local directory where source code is located
    :param s3_dest: the AWS S3 location where files will be uploaded
    :param optional_source: a comma separated list of optional files to be uploaded
    :return: True if configuration is validated; False otherwise
    """
    # upload_config is a namedtuple of type UploadConfig:
    #  UploadConfig = namedtuple('UploadConfig', ['flag','source','dest','optional_source'])
    #  Note that 'optional_source' could be a comma separated string
    section = section_config.name

    for item in [source, s3_dest, flag]:
        if item not in section_config:
            print(ERROR + "Invalid entry in configuration file section: " + CYAN + "[" + section + "]" + NC)
            print("Item missing:\t" + YELLOW + item + NC)
            return False

    # flag must be either 'True' or 'False'
    flag_value = section_config.get(flag, "NOT_SET")
    if flag_value not in ['True', 'False']:
        print(ERROR + "Invalid entry in configuration file section: " + CYAN + "[" + section + "]" + NC)
        print("\t" + flag + " = " + YELLOW + flag_value + NC)
        print("This item value is required to be either 'True' or 'False'")
        return False

    # script names are required to be pathless filenames
    source_items = [source]
    # add optional files - if exits
    if optional_source in section_config:
        # note that we are just dealing with item names at this stage
        source_items.append(optional_source)
    # now check for pathless file names
    for item in source_items:
        # Even though one of these items could be a comma separated string -
        # no need to convert to a list - as we are just looking for
        # the "/" character anywhere in the string
        file_name = section_config[item]
        if '/' in file_name:
            print(ERROR + "Invalid entry in config file section: " + CYAN + "[" + section + "]" + NC)
            print("\t" + item + " = " + YELLOW + file_name + NC)
            print("This field value is required to be a pathless file name")
            return False

    # dest must start with a valid s3 bucket regardless of upload
    # flag status
    s3_dest_value = section_config[s3_dest]
    if not is_valid_s3_bucket(s3_dest_value):
        print(ERROR + "Invalid entry in config file section: " + CYAN + "[" + section + "]" + NC)
        print("\t" + s3_dest + " = " + YELLOW + s3_dest_value + NC)
        print("This field value is required to start with \"s3://<bucket>\" - where <bucket> is an existing s3 bucket.")
        return False

    # If the upload flag status is set to False, then that implies that
    # all files have already been uploaded & exist - check this
    if section_config[flag] != "True":
        source_files = []
        for item in source_items:
            source_files += ([x.strip() for x in
                              section_config[item].split(',')])
        for f in source_files:
            if not check_s3_file_exists(s3_dest_value, f):
                print(ERROR + "Invalid entry in config file section: " + CYAN + "[" + section + "]" + NC)
                print("\t" + YELLOW + f + NC)
                print("This file does not exist in " + YELLOW + s3_dest_value + NC)
                print("Either change upload flag to True or supply an existing filename / location.")
                return False
    else:
        local_dir_value = section_config.get(local_dir, "")
        if local_dir_value == "":
            print(ERROR + "Invalid entry in config file section: " + CYAN + "[" + section + "]" + NC)
            print("Item missing\t" + YELLOW + local_dir + NC)
            print("You need to specify " + YELLOW + local_dir + NC + " if you set " + YELLOW + flag + NC + " to True!")
            return False
        elif not os.path.isdir(local_dir_value):
            print(ERROR + "Invalid entry in config file section: " + CYAN + "[" + section + "]" + NC)
            print("\t" + local_dir + " = " + YELLOW + local_dir_value + NC)
            print("This path provided is not a valid folder.")
            return False

    return True


def upload_files_to_s3(file_list, dry_run=False):
    """
    uploads files to an AWS S3 bucket
    :param file_list: list of files to be uploaded
    :param dry_run: a boolean flag for dry-run; no upload if set to False
    :return: a comma separated list of upload files
   """
    s3_client = boto3.client("s3")
    uploaded_files = []

    for name, local_dir, s3_dest in file_list:
        file_location = local_dir.rstrip("/") + "/" + name
        bucket_name, key_prefix = s3_dest.strip().strip("/")[5:].split("/", 1)

        if not dry_run:
            s3_client.upload_file(file_location, bucket_name, key_prefix + "/" + name)

        uploaded_files.append(s3_dest.rstrip("/") + "/" + name)

    return ",".join(uploaded_files)


def is_valid_s3_bucket(s3_string):
    """
    Determine if the input string starts with a valid s3 bucket name
    :param s3_string: an aws s3 address (e.g. s3://mybucket/other...)
    :return: True if the s3_string contains a valid bucket name
    """
    client = boto3.client('s3')
    # only applies to s3 - so ignore otherwise
    if s3_string[0:5] != 's3://':
        return False
    # get the bucket name
    bucket = s3_string[5:].strip('/').split('/')[0]
    if not bucket:
        return False
    # see if bucket exists
    try:
        client.list_objects(Bucket=bucket)
    except:
        return False

    return True


def check_s3_file_exists(s3_path, file_name):
    """
    Determine if a s3 key exists
    :param s3_path: an s3 "directory" path (e.g. s3://mybucket/name/)
    :param file_name: a pathless file name (e.g. myfile.txt)
    :return: True if key exists; False otherwise
    """
    full_path = s3_path.rstrip('/') + '/' + file_name
    bucket_name, key_prefix = full_path[5:].split("/", 1)
    client = boto3.client('s3')
    # see if file exists
    try:
        client.get_object(Bucket=bucket_name, Key=key_prefix)
    except:
        return False

    return True


def check_s3_path_exists(s3_locations):
    """
    Determine if any s3 key exists that begins with the given path
    :param s3_locations: a list of AWS S3 locations ("directories")
    :return: a list of existing s3 paths (subset of input)
    """
    client = boto3.client('s3')
    ret_vals = []
    for loc in s3_locations:
        # only applies to s3 - so ignore otherwise
        if loc[0:5] != 's3://':
            raise ValueError("Invalid s3 string - expecting s3 string to start with \"s3://\":  {}".format(loc))
        try:
            bucket, key = loc[5:].rstrip('/').split("/", 1)
        except ValueError:
            raise ValueError("Invalid s3 string - output location can't be a bucket name: {}".format(loc))
        key += '/'
        # remove the files matching the prefix
        try:
            obj_list = (client.list_objects(Bucket=bucket,
                                            Prefix=key)['Contents'])
        except KeyError:
            # when creating list with client.list_objects, ['Contents']
            # won't be available if key doesn't exist.  This is OK.
            continue
        ret_vals.append(loc)

    return ret_vals


def remove_s3_files(s3_locations):
    """
    recursively removes files from an AWS S3 location
    :param s3_locations: a list of AWS S3 locations ("directories")
    :return: True if successful; False otherwise
    """

    client = boto3.client('s3')
    for loc in s3_locations:
        # only applies to s3 - so ignore otherwise
        if loc[0:5] != 's3://':
            print(ERROR + ' Invalid s3 prefix in: ' +
                  YELLOW + loc + ".  Expected prefix \"s3://\"")
            return False
        try:
            bucket, key = loc[5:].split("/", 1)
        except ValueError:
            print(ERROR + "Invalid s3 location: " +
                  YELLOW + loc + NC + "Cannot remove whole bucket")
            return False
        key += '/'
        # remove the files matching the prefix
        try:
            obj_list = (client.list_objects(Bucket=bucket,
                                            Prefix=key)['Contents'])
            for s3_key in [x['Key'] for x in obj_list]:
                client.delete_object(Bucket=bucket, Key=s3_key)
        except botocore.exceptions.ClientError as e:
            error_code = e.response['Error']['Code']
            # pick out invalid bucket name
            if error_code == 'NoSuchBucket':
                print(ERROR + "Invalid s3 location: " +
                      YELLOW + loc + NC + " bucket does not exist")
            else:
                print(ERROR + "Invalid s3 location: " +
                      YELLOW + loc + NC + " " + error_code)
            return False
        except KeyError:
            # when creating list with client.list_objects, ['Contents']
            # won't be available if key doesn't exist.  This is OK.
            continue
        except Exception as e:
            print(ERROR + 'An error occurred while attempting to remove s3 files at location:' +
                  YELLOW + loc)
            print(e)
            return False

    return True


def get_cluster_mem_cpu(cluster_id):
    """
    gets the total memory and total number of CPU's in the cluster
    :param cluster_id: the id of the cluster to examine
    :return: a tuple of total memory and number of CPU's ;(-1, -1) on error
    """
    # currently no error handling - at this stage more useful
    # to let system print error message
    inst_stats = {}
    # instance types file is in format:
    # <inst type> <mem in Gb> <# cpus>
    with open(INSTANCE_TYPES_FILE, 'r') as f:
        lines = f.readlines()
        for line in lines:
            inst_type, mem, cpu = line.split()
            inst_stats[inst_type] = (float(mem), int(cpu))

    # reset the variable types from string to what they should be
    mem = float(0)
    cpu = 0

    client = boto3.client('emr')
    ec2_client = boto3.client('ec2')
    group_types = ['CORE', 'TASK']
    # for each instance in the cluster nodes
    try:
        for instance in client.list_instances(ClusterId=cluster_id,
                                              InstanceGroupTypes=group_types)['Instances']:
            # get the instance id
            inst_id = instance['Ec2InstanceId']
            try:
                # get the instance type
                inst_type = ec2_client.describe_instances(
                    InstanceIds=[inst_id])['Reservations'][0]['Instances'][0]['InstanceType']
                mem += inst_stats[inst_type][0]
                cpu += inst_stats[inst_type][1]
            except:
                continue
    except botocore.exceptions.ClientError:
        # could be a dry run
        mem = -1.0
        cpu = -1

    return mem, cpu


def get_number_of_lines_s3_file(s3_path, region):
    """
    get number of lines in a file stored on s3
    Designed for use with a "text" manifest file
    - where the number of lines in the file may correspond to an upper
    limit to the number of mappers for a preprocessing job
    :param s3_path: AWS S3 path
    :param region: AWS S3 region
    :return: number of lines in file (-1 indicates failure)
    """
    lines = -1
    # temp dir cleaned up automatically
    with tempfile.TemporaryDirectory() as tmpdir:
        try:
            client = boto3.client('s3', region)
            transfer = S3Transfer(client)
            # Download file to temporary directory
            bucket, key = s3_path[5:].split('/', 1)
            file_name = tmpdir + '/' + s3_path.split('/')[-1]
            transfer.download_file(bucket, key, file_name)
            # read the file to see how many lines
            text_file = open(file_name, "r")
            lines = text_file.readlines()
            return len(lines)
        except:
            pass
    return lines
