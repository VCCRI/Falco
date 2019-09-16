#!/bin/bash

count=1
spot_price=5
launch_spec_json_file=ec2-ami-spec.json
user_data_file=user-data-ami.sh
tmp_json_file=tmp.json

#colours; need to use -e option with echo
#red='\e[0;31m'
#cyan='\e[0;36m'
#green='\e[0;32m'
#yellow='\e[1;33m'
#purple='\e[0;35m'
#nc='\e[0m' #no colour
red=$( tput setaf 1 )
cyan=$( tput setaf 6 )
green=$( tput setaf 2 )
yellow=$( tput setaf 3 )
purple=$( tput setaf 5 )
nc=$( tput sgr0 )

#program usage
usage() {
    echo -e "${red}program exited due to invalid usage${nc}"
    echo -e "${yellow}usage:${purple} $0 ${cyan}--instance-type <type> \
        <count> [[--instance-type <type> <count>]...] \
        [--user-data <value>] [--dry-run]${nc}"
    echo -e "${yellow}Example:${nc}"
    echo -e "$0 --instance-type r3.large 1 --user-data some-file.sh --dry-run"
    echo -e "${yellow}Valid instance types:${cyan}"
    echo -e "${nc}"
    exit 1
}

#checks latest return status
#accepts one argument - name of program call
check_status() {
    if [ $? -ne 0 ] ; then
        echo -e "${red}program exited due to unsuccessful excecution: \
            ${cyan}${1}${nc}"
        exit 1
    fi
}

#checks program usage: assumes 1 parameter: modify as necessary
#if [ $# -lt 3 ] ; then
#    usage
#fi

#function to exit program with message
exit_msg() {
    echo -e "${red}Exiting program: ${cyan}${1}${nc}"
    exit 1
}

start=`date +%s` #seconds since epoc

# remove old instance id file
ids_file=ids.txt
if [[ -f $ids_file ]] ; then
    rm $ids_file
fi

# file to hold instance public ip addresses
ips_file=ips.txt
if [[ -f $ips_file ]] ; then
    rm $ips_file
fi

# file to hold spot instance request ids
spot_request_ids_file=spot-ids.txt
if [[ -f $spot_request_ids_file ]] ; then
    rm $spot_request_ids_file
fi

# create base64 code for user data
# NOTE difference in base64 between mac platform & other linux
# mac base64 uses -b option iso -w option
op="-w"
v=$( man base64 | grep '\-w' )
[ $? -ne 0 ] && op="-b"
user_data=$( base64 $op 0 $user_data_file )
check_status "creating user data"
# update the base64 user-data string in the .json file
awk -F":" -v user_data=$user_data -v json_file=$tmp_json_file '{
    if ($1 ~ /UserData/)
        print $1 ":\"" user_data "\"" > json_file
    else
        print $0 > json_file
} END {
close (json_file)
}' $launch_spec_json_file
check_status "replacing user data"
mv $tmp_json_file $launch_spec_json_file

#create the instances that were requested
aws ec2 request-spot-instances \
    --spot-price $spot_price \
    --instance-count $count \
    --type "one-time" \
    --launch-specification file://$launch_spec_json_file \
    --output text \
    --query 'SpotInstanceRequests[*].SpotInstanceRequestId' > \
        $spot_request_ids_file

if [[ $? -ne 0 ]] ; then
    exit_msg "spot request command failed"
fi
echo -e "${yellow}Waiting for spot requests to be fulfilled${nc}"
while (true) ; do
    sleep 10
    fulfilled=0
    for request_id in `cat $spot_request_ids_file` ; do
        # the effect of this look is to count the # of fulfilled
        # spot requests
        fulfilled=$((`aws ec2 describe-spot-instance-requests \
                    --spot-instance-request-ids $request_id \
                    --output text \
                    --query "SpotInstanceRequests[*].Status.Code" \
                    |grep "fulfilled"|wc -l|awk '{print $1}'` + \
                    $fulfilled))
    done
    if [[ $fulfilled -eq $count ]] ; then
        # record instance_ids
        for request_id in `cat $spot_request_ids_file` ; do
            aws ec2 describe-spot-instance-requests \
                --spot-instance-request-ids $request_id \
                --output text \
                --query "SpotInstanceRequests[*].InstanceId" >> \
                $ids_file
        done
        break
    fi
done
echo -e "${green}All spot requests have been fulfilled${nc}"
all_done=true

#wait a minute for the instances to start running
sleep 60
#start an infinite loop to check when instances are running
while true; do
    all_done=true
    #check the run state of each instance id that was created
    if [[ -f $ips_file ]] ; then
        rm -f $ips_file
    fi
    for id in `cat $ids_file`; do
        #check the instance reachability status - when "passed"
        #should be ok to use
        instance_details_name=`aws ec2 describe-instance-status \
            --output text \
            --instance-ids $id --query \
            'InstanceStatuses[0].InstanceStatus.Details[0].Name'`
        instance_details_status=`aws ec2 describe-instance-status \
            --output text \
            --instance-ids $id --query \
            'InstanceStatuses[0].InstanceStatus.Details[0].Status'`
        if ! [[ ("$instance_details_name" == "reachability") &&
           ("$instance_details_status" == "passed") ]] ; then
            all_done=false
            #this instance is not ready
            break
        fi
        ipaddr=`aws ec2 describe-instances --instance-ids --output text $id --query \
            'Reservations[0].Instances[0].PublicDnsName'`
        inst_type=`aws ec2 describe-instances --instance-ids --output text $id --query \
            'Reservations[0].Instances[0].InstanceType'`
        echo $ipaddr >> $ips_file
    done
    if ! $all_done ; then
        sleep 10
    else
        break
    fi
done

finish=`date +%s` #seconds since epoc
echo -e "${yellow}time: ${cyan}$(( $finish - $start ))${nc}"
cat $ips_file
