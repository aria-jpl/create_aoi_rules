#!/usr/bin/env python

'''
Create COR Network Selection trigger rule and submit iteration job
'''

import os
import json
import datetime
import dateutil.parser
import pytz
import add_rule

CONDITION = 'cor/cor.condition.json'
KEYWORD_ARGS = 'cor/cor.keyword-args.json'
JOB_NAME = 'hysds-io-s1-cor:{0}'
RULE_NAME = 'cor_{0}_rule'

def main():
    '''main loop, parses context & uses params to create cor trigger rule
    and then submit an COR iteration job'''
    context = load_context()
    args = build_keyword_args(context)
    condition = build_condition(context)
    rule_name = RULE_NAME.format(context['aoi_name'])
    queue = context['cor_job_queue']
    priority = int(context['cor_job_priority'])
    job = JOB_NAME.format(context['cor_job_version'])
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
        raise Exception('unable to parse {0} from work directory'.format(input_filename))

def build_keyword_args(ctx):
    '''fills appropriate keyword args from context and returns the object'''
    keyword_args = load_config(KEYWORD_ARGS)
    arg_list = []
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

def get_baseline(starttime_string, endtime_string):
    '''generates the temporal baseline from tbe aoi time range'''
    starttime = dateutil.parser.parse(starttime_string).replace(tzinfo=pytz.UTC)
    endtime = dateutil.parser.parse(endtime_string).replace(tzinfo=pytz.UTC)
    tot_baseline = int(abs((endtime - starttime).days))
    return tot_baseline

if __name__ == '__main__':
   main()
