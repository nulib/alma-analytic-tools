#!/usr/bin/env python
"""
Retrieve all data (even more than 65001 rows) from an Alma analytic

This file is an example of how to perform query all with optional
multithreading by using a QueryFactory along with RequestObject and
AnalyticAgent.

This script requires the user to provide an input file for a RequestObject.
The RequestObject will be loaded as a complex request. 
"""

##########################################################################
# Copyright (c) 2014 Katherine Deibel
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED 'AS IS' AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
##########################################################################

from __future__ import print_function

import codecs, getopt, os, sys

from AnalyticAgent import AnalyticAgent
from HathiTrustAgents import BaseHathiAgent, HathiSerialAgent, HathiMPMAgent, HathiSPMAgent
from RequestObject import RequestObject
from QueryFactory import QueryFactory


# List the options/flags for query_analytic to make it easier to
# generate the help message.
options = {}
options[u'-h, --help'] = u'Display this help and exit'
options[u'-d, --debug'] = u'Print titles in the output'
options[u'-n, --threads=N'] = u'Run the query using N parallel threads. Default: 1'
options[u'-q, --quiet'] = u'Quiet mode. Logging only written to file and not the screen'
options[u'-r, --resume'] = u'Re-run the request but skip any previously completed jobs'
options[u'-i, --input=path'] = u'Path to directory for input files. Default: ./hathi_inputs'
        
required = {}
required[u'--serial'] = u'Download serial holdings (hathi_serials.txt)'
required[u'--spm'] = u'Download single-part monograph holdings (hathi_spms.txt)'
required[u'--mpm'] = u'Download multi-part monograph holdings (hathi_mpms.txt)'

def print_help():
    """Print the help message."""
    print(u'Usage: hathi_download.py [options] [holding type]')
    
    print(u'Options:')

    _opts = options.keys()
    _opts.sort()
    # get the length of the longest key for formatting purposes
    n = len( max(_opts, key=len) )

    for o in _opts:
        print( u' ' + o.ljust(n+2,u' ') + options.get(o) )

    print(u'And exactly ONE of the following is required:')
    _reqs = required.keys()
    _reqs.sort()
    for r in _reqs:
        print( u' ' + r.ljust(n+2,u' ') + required.get(r) )


def main(argv):
    """Main method that processes the command line, loads the
    request object, initializes the factory, and then starts
    it up.
    """
    # process the command line    
    shortopts = 'hdqrn:i:'
    longopts = ['help','debug', 'quiet', 'threads=', 'input=', 'resume', 'serial', 'mpm', 'spm']
    try:
        opts, args = getopt.gnu_getopt(argv, shortopts, longopts)
    except getopt.GetoptError as err:
        print(unicode(err), file=sys.stderr)
        print_help()
        sys.exit(2)

    # check that a filename was not provided
    if len(args) != 1: # query_analytic.py, filename
        print_help()
        sys.exit(2)

    # set features according to options
    allowZeros = False
    numThreads = 1
    screenLogging = True
    resumeWork = False
    printTitles = False
    fileStem = 'data'
    holdingType = None
    agtClass = AnalyticAgent
    inputPath = 'hathi_inputs/'
   
    # process the options now              
    for opt, arg in opts:
        if opt == '-h' or opt == '--help':
            print_help()
            sys.exit(0)
        elif opt == '-n' or opt == '--threads':
            numThreads = int(arg)
            if numThreads < 1:
                print(u'The number of threads must be at least 1!',
                      file=sys.stderr)
                sys.exit(2)
        elif opt == '-i' or opt == '--input':
            inputPath = arg.strip()
            # make sure there is a slash at end of path            
            if inputPath[-1] != '/': 
                inputPath += '/'
            # check directory existence
            if not os.path.isdir(inputPath):
                print(u'Path: ' + inputPath + u' is not a path / is inaccessible',
                      file=sys.stderr)
                sys.exit(2)            
        elif opt == '-q' or opt == '--quiet':
            screenLogging = False
        elif opt == '-r' or opt == '--resume':
            resumeWork = True
        elif opt == '-d' or opt == '--debug':
            printTitles = True            
        elif opt == '-z' or opt == '--zero':
            allowZeros = True
        elif opt == '--serial':
            holdingType = 'serial'
            agtClass = HathiSerialAgent
        elif opt == '--spm':
            holdingType = 'spm'
            agtClass = HathiSPMAgent            
        elif opt == '--mpm':
            holdingType = 'mpm'
            agtClass = HathiMPMAgent

    # check that only one holding type is given
    n = 0
    for opt, arg in opts:
        if opt in ['--serial', '--spm', '--mpm' ]:
            n = n + 1
    if n == 0:
        print(u'You must include a holding type: --serial, --spm, or --mpm\n',
              file=sys.stderr)
        print_help()
        sys.exit(2)
    elif n > 1:
        print(u'You can only request one holding type at a time!',
              file=sys.stderr)

    # set various parameters based on the holding type
    fileStem = holdingType
    requestFile = inputPath + 'hathi_' + holdingType + 's.txt'

    # check that requestFile exists
    if not os.path.isfile(requestFile):
        print('Unable to read input file: ' + requestFile,
              file=sys.stderr)
        sys.exit(2)
    
    # attempt to load the request object
    try:
        request = RequestObject.fromFilename(requestFile,simpleRequest=False)
    except Exception as e:
        lines = unicode(e).splitlines()
        if len(lines) == 1:
            print(u'Formatting error detected.', file=sys.stderr)
            print(u'--> ' + lines[0], file=sys.stderr)
        else:
            print( lines[0], file=sys.stderr)
            for line in lines[1:]:
                print(u'--> ' + line, file=sys.stderr)
        sys.exit(1)

    print(u'Loaded analytic request: ' + requestFile)
    numJobs = len(request.JobBounds) - 1

    print(u'Jobs to complete: ' + unicode(numJobs))
        
    # check the numbers with the processors
    if numJobs < numThreads:
        numThreads = numJobs
        print(u'Parallel threads: ' + unicode(numThreads) \
              + u' (reduced to number of jobs)')
    else:
        print(u'Parallel threads: ' + unicode(numThreads))

    print(u'Output files: ')
    print(u'   ' + unicode(numJobs) + ' data file(s): ' \
          +agtClass.data_filename(fileStem,id=u'#'))

    print(u'   ' + u'1 file mapping job numbers to bounds: ' \
          + QueryFactory.jobmap_filename(fileStem) )

    print(u'   ' + u'1 log file: ' 
          + QueryFactory.log_filename(fileStem) )

    print(u'   ' + unicode(numJobs) + ' no OCLC file(s): ' \
          + agtClass.data_filename('no-oclc-' + fileStem,u'#'))


    # before starting the factory, set the BaseHathiAgent for debugging
    BaseHathiAgent.PRINT_TITLES = printTitles

    factory = QueryFactory.setup(request=request, num_workers=numThreads, 
                           file_stem=fileStem, to_standard_out=screenLogging, 
                           allow_zeros=allowZeros, resume_work=resumeWork, 
                           agent_class=agtClass)
    print()
    print(u'Grabbing Analytic data (press Ctrl+C to quit)...')
    print()

    if factory.engage():
        sys.exit(0)
    else:
        sys.exit(1)

if __name__ == '__main__':
    main(sys.argv)
