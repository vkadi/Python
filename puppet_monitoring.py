#!/usr/bin/python

from pypuppetdb import connect
import urllib
import json
import os 
import requests
import subprocess 

def main():
    account_ids = get_accountids()
    for account in account_ids:
        nodes = get_puppetnodes(account)
        print nodes
        total_no_of_nodes = len(nodes)
        print(total_no_of_nodes)
        failure = 0
        active = 0
        for id in nodes:
            avail_zone = get_availability_zone(id)
            status = get_puppetnode_status(id)
            if status == "failed":
                failure = failure + 1
            else:
                active = active + 1 
               
        print "No of failed nodes in account %d" %failure
        print "No of Passed nodes in account %d" %active
        print "Metrics for Account %s" %account
         
        metric1 = 'echo  "Active_Node %d" | curl --data-binary @- http://10.128.4.151:9091/metrics/job/puppet_monitoring/aws_account_id/%s' %(active,account)
        metric2 = 'echo  "Failure_Node %d" | curl --data-binary @- http://10.128.4.151:9091/metrics/job/puppet_monitoring/aws_account_id/%s' %(failure,account)
        metric3 = 'echo  "Puppet_clients_Running %d" | curl --data-binary @- http://10.128.4.151:9091/metrics/job/puppet_monitoring/aws_account_id/%s' %(total_no_of_nodes,account)
        subprocess.call(metric1, shell=True)
        subprocess.call(metric2, shell=True)
        subprocess.call(metric3, shell=True)


def get_accountids():
    """
    get all account ids using metadata api
    : accountid : aws account no
    """
    all_accounts = []
    result = os.popen("curl https://metadata.aws.xyz.com/api/accounts/").read()
    api_data = json.loads(result)
    for item in api_data:
        ids = item['account_id']
        all_accounts.append(ids.encode('ascii','ignore'))
    return all_accounts 



def get_puppetnode_status(id):
    """
    get the status of puppet nodes
    :param : PuppetDB query parameters 
    : accountid : aws account no
    """
    params = (
        ('query', '["=", "certname", "%s"]' %id), 
    )
    response = requests.get('http://localhost:8080/pdb/query/v4/nodes', params=params)
    status = ""
    result = response.content 
    node_status = json.loads(result)
    for item in node_status:
        status = item['latest_report_status']
    return status 


def get_puppetnodes(accountid):
    """
    get puppet clients associated with particular account
    :param hostname: PuppetDB hostname
    : accountid : aws account no
    """
    nodes = []
    params = (
        ('query', '["and",["=","name","aws_account_id"],["~", "value", "%s"]]' %accountid),
    )
    response = requests.get('http://localhost:8080/pdb/query/v4/facts', params=params)
    result = response.content
    all_nodes = json.loads(result)
    for item in all_nodes:
        node = item['certname']
        nodes.append(node.encode('ascii','ignore'))
    return nodes

def get_availability_zone (id):
    """
    get the availability-zone of puppet node
    :param : PuppetDB query parameters 
    : accountid : aws account no
    """
    params = (
        ('query', '["=", "certname", "%s"]' %id), 
    )
    response = requests.get('http://localhost:8080/pdb/query/v4/facts', params=params)
    status = ""
    result = response.content 
    node_status = json.loads(result)
    zone = ""
    for i in range(len(node_status)):
        for k,v in node_status[i].items():
            if 'placement' in str(v):
                zone_output = v['placement']
                zone = zone_output['availability-zone']
    return zone 

if __name__ == "__main__":
    main()
