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

CONDITION = 'slcp/slcp.condition.json'
KEYWORD_ARGS = 'slcp/slcp.keyword-args.json'
JOB_NAME = 'hysds-io-sciflo-s1-slcp:{0}'
RULE_NAME = 'slcp_{0}_rule'

def main():
    '''main loop, parses context & uses params to create slcp trigger rule
    and then submit an COD iteration job'''
    context = load_context()
    args = build_keyword_args(context)
    condition = build_condition(context)
    rule_name = RULE_NAME.format(context['aoi_name'])
    queue = context['slcp_job_queue']
    priority = int(context['slcp_job_priority'])
    job = JOB_NAME.format(context['slcp_job_version'])
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
    arg_list = ['dataset_tag', 'project', 'singlesceneOnly', 'preReferencePairDirection', 'postReferencePairDirection',
    'minMatch', 'covth', 'precise_orbit_only', 'azimuth_looks', 'filter_strength', 'dem_type']
    for key in arg_list:
        keyword_args[key] = ctx[key]
    keyword_args['temporalBaseline'] = get_baseline(ctx['starttime'], ctx['endtime'])
    return keyword_args

def build_condition(ctx):
    '''fills the condition object with proper args from context'''
    condition = load_config(CONDITION)
    condition['filtered']['query']['bool']['must'][1]['range']['metadata.sensingStart']['from'] = ctx['starttime']
    condition['filtered']['query']['bool']['must'][1]['range']['metadata.sensingStart']['to'] = ctx['endtime']
    condition['filtered']['filter']['geo_shape']['location']['shape'] = ctx['location']
    return condition

def get_baseline(starttime_string, endtime_string):
    '''generates the temporal baseline from tbe aoi time range'''
    starttime = dateutil.parser.parse(starttime_string).replace(tzinfo=pytz.UTC)
    endtime = dateutil.parser.parse(endtime_string).replace(tzinfo=pytz.UTC)
    tot_baseline = int(abs((endtime - starttime).days))
    return tot_baseline

if __name__ == '__main__':
    main()
