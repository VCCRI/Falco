#!/bin/bash
ip=$( head -1 ips.txt )
ssh -i [PRIVATE_KEY] ec2-user@$ip
