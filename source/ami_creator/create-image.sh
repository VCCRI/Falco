#!/bin/bash
id=$( head -1 ids.txt )
aws ec2 create-image --instance-id $id \
    --name "Falco custom AMI" \
    --description "Custom AMI for Falco framework with tools pre-installed" \
    --query 'ImageId' > custom_ami_id.txt
