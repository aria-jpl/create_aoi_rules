#!/usr/bin/env python

import sys
from hysds_commons import *


def main(rule):
    '''submit iterate job for given rule'''
    hysds_commons.iterate('tosca', rule)

if __name__ == '__main__':
    main(sys.argv[1])
    
