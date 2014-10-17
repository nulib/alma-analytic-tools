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
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
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
from RequestObject import RequestObject
from QueryFactory import QueryFactory


# List the options/flags for query_analytic to make it easier to
# generate the help message.
options = {}
options[u"-h, --help"] = u"Display this help and exit."
options[u"-z, --zero"] = u"Allow analytics to return zero results."
options[u"-n, --threads=N"] = u"Run the query using N parallel threads. Default: 1."
options[u"-q, --quiet"] = u"Quiet mode. Logging only written to file and not the screen."
options[u"-r, --resume"] = u"Re-run the request but skip any previously completed jobs."
options[u"-s, --stem=str"] = u"Ouput files begin with str as the filestem. Default:'data'"

def print_help():
    """Print the help message."""
    print(u"Usage: download_all.py [options] <requestfile>")
    
    print(u"Options:")

    _opts = options.keys()
    _opts.sort()
    # get the length of the longest key for formatting purposes
    n = len( max(_opts, key=len) )

    for o in _opts:
        print( u" " + o.ljust(n+2,u' ') + options.get(o) )


def main(argv):
    """Main method that processes the command line, loads the
    request object, initializes the factory, and then starts
    it up.
    """
    # process the command line    
    shortopts = "hzqrs:n:"
    longopts = ["help","stem=", "quiet", "zero", "threads=", "resume"]
    try:
        opts, args = getopt.gnu_getopt(argv, shortopts, longopts)
    except getopt.GetoptError:
        print_help()
        sys.exit(2)

    # check that a filename was provided
    if len(args) != 2: # query_analytic.py, filename
        print_help()
        sys.exit(2)

    # set features according to options
    allowZeros = False
    numThreads = 1
    screenLogging = True
    resumeWork = False
    fileStem = "data"
    
    for opt, arg in opts:
        if opt == "-h" or opt == "--help":
            print_help()
            sys.exit(0)
        elif opt == "-n" or opt == "--threads":
            numThreads = int(arg)
            if numThreads < 1:
                print(u"The number of threads must be at least 1!",
                      file=sys.stderr)
                sys.exit(2)
        elif opt == "-q" or opt == "--quiet":
            screenLogging = False
        elif opt == "-r" or opt == "--resume":
            resumeWork = True
        elif opt == "-s" or opt == "--stem":
            fileStem = arg            
        elif opt == "-z" or opt == "--zero":
            allowZeros = True

    requestFile = args[1]
    # check that requestFile is a file
    if not os.path.isfile(requestFile):
        print(u"File: " + requestFile + u" could not be found.",
              file=sys.stderr)
        sys.exit(1)
    
    # attempt to load the request object
    try:
        request = RequestObject.fromFilename(requestFile,simpleRequest=False)
    except Exception as e:
        lines = unicode(e).splitlines()
        if len(lines) == 1:
            print(u"Formatting error detected.", file=sys.stderr)
            print(u"--> " + lines[0], file=sys.stderr)
        else:
            print( lines[0], file=sys.stderr)
            for line in lines[1:]:
                print(u"--> " + line, file=sys.stderr)
        sys.exit(1)

    print(u"Loaded analytic request: " + requestFile)
    numJobs = len(request.JobBounds) - 1

    print(u"Jobs to complete: " + unicode(numJobs))
        
    # check the numbers with the processors
    if numJobs < numThreads:
        numThreads = numJobs
        print(u"Parallel threads: " + unicode(numThreads) \
              + u" (reduced to number of jobs)")
    else:
        print(u"Parallel threads: " + unicode(numThreads))

    print(u"Output files: ")
    print(u"   " + unicode(numJobs) + " data file(s): " \
          + AnalyticAgent.data_filename(fileStem,u'#'))

    print(u"   " + u"1 file mapping job numbers to bounds: " \
          + QueryFactory.jobmap_filename(fileStem) )

    print(u"   " + u"1 log file: " 
          + QueryFactory.log_filename(fileStem) )
    
    factory = QueryFactory.setup(request=request, num_workers=numThreads, 
                           file_stem=fileStem, to_standard_out=screenLogging, 
                           allow_zeros=allowZeros, resume_work=resumeWork, 
                           agent_class=AnalyticAgent)
    print()
    print(u"Grabbing Analytic data (press Ctrl+C to quit)...")
    print()

    if factory.engage():
        sys.exit(0)
    else:
        sys.exit(1)

if __name__ == "__main__":
    main(sys.argv)
