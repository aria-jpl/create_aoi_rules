#!/usr/bin/env python

'''
Create COD Network Selection trigger rule and submit iteration job
'''

import os
import json
import datetime
import dateutil.parser
import pytz
import add_rule

CONDITION = 'cod/cod.condition.json'
KEYWORD_ARGS = 'cod/cod.keyword-args.json'
JOB_NAME = 'hysds-io-slcp2cod_network_selector:{0}'
RULE_NAME = 'cod_{0}_rule'

def main():
    '''main loop, parses context & uses params to create cod trigger rule
    and then submit an COD iteration job'''
    context = load_context()
    args = build_keyword_args(context)
    condition = build_condition(context)
    rule_name = RULE_NAME.format(context['aoi_name'])
    queue = context['cod_job_queue']
    priority = int(context['cod_job_priority'])
    job = JOB_NAME.format(context['cod_job_version'])
    user = context['user']
    add_rule.add_rule(rule_name, condition, job, queue, priority, args, user)
    if context['submit_job'] is True:
        #submit an on-demand job
        pass

def load_context():
    '''load context.json'''
    try:
        context_file = '_context.json'
        with open(context_file, 'r') as fin:
            context = json.load(fin)
        return context
    except:
        raise Exception('unable to parse context.json from work directory')

def load_config(input_filename):
    '''load input json from the config directory'''
    input_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config', input_filename)
    try:
        with open(input_path, 'r') as fin:
            cfg = json.load(fin)
        return cfg
    except:
        raise Exception('unable to parse context.json from work directory')

def build_keyword_args(ctx):
    '''fills appropriate keyword args from context and returns the object'''
    keyword_args = load_config(KEYWORD_ARGS)
    arg_list = ['dataset_tag', 'project', 'slcp_version', 'aoi_name', 'minmatch']
    for key in arg_list:
        keyword_args[key] = ctx[key]
    return keyword_args

def build_condition(ctx):
    '''fills the condition object with proper args from context'''
    condition = load_config(CONDITION)
    condition['filtered']['query']['bool']['must'][2]['range']['metadata.sensingStart']['from'] = ctx['starttime']
    condition['filtered']['query']['bool']['must'][2]['range']['metadata.sensingStart']['to'] = ctx['endtime']
    condition['filtered']['filter']['geo_shape']['location']['shape'] = ctx['location']
    if 'track_number' in list(ctx.keys()) and ctx['track_number'] is not None and ctx['track_number'] != '':
        condition['filtered']['query']['bool']['must'][1]['term']['metadata.trackNumber'] = ctx['track_number']
    else:
        del condition['filtered']['query']['bool']['must'][1]
    return condition


if __name__ == '__main__':
    main()
