#!/usr/bin/env python
"""
Retrieve a report (65001 rows) from an Alma analytic

This file is an example of how to perform a report-only query by using the
helper classes RequestObject and AnalyticAgent.

This script requires the user to provide an input file for a RequestObject.
The RequestObject will be loaded as a simple request (i.e., no need for
uniqueID, sortedBy*, jobBounds) but the NamesToColumns mapping can be
included.
"""

from __future__ import print_function

import codecs, getopt, os, sys

from AnalyticAgent import AnalyticAgent, QueryType
from RequestObject import RequestObject
from QueryFactory import QueryFactory
from CustomExceptions import AnalyticServerError, ZeroResultsError

# List the options/flags for query_analytic to make it easier to
# generate the help message.
options = {}
options[u"-h, --help"] = u"Display this help and exit."
options[u"-z, --zero"] = u"Allow analytics to return zero results."
options[u"-q, --quiet"] = u"Quiet mode. Minimal output to screen."
options[u"-s, --stem=str"] = u"Ouput files begin with str as the filestem. Default:'data'"

def print_help():
    """Print the help message."""
    print(u"Usage: report_download.py [options] <requestfile>")
    
    print(u"Options:")

    _opts = options.keys()
    _opts.sort()
    # get the length of the longest key for formatting purposes
    n = len( max(_opts, key=len) )

    for o in _opts:
        print( u" " + o.ljust(n+2,u' ') + options.get(o) )
#end print_help()

def main(argv):
    """Main method that processes the command line, loads the request object,
    creates an agent, and then starts the query.
    """
    
    # process the command line    
    shortopts = "hzqs:"
    longopts = ["help","stem=", "quiet", "zero",]
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
    screenLogging = True
    fileStem = "data"
    
    for opt, arg in opts:
        if opt == "-h" or opt == "--help":
            print_help()
            sys.exit(0)
        elif opt == "-q" or opt == "--quiet":
            screenLogging = False
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
    
    # attempt to load the request object as a simple request
    try:
        request = RequestObject.fromFilename(requestFile,simpleRequest=True)
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

    # get the output filename
    filename = AnalyticAgent.data_filename(fileStem)
    print(u"Loaded analytic request: " + requestFile)
    print(u"Output files: " + filename)

    # create and run the agent
    print()
    print(u"Grabbing Analytic data...")
    try:
        agt = AnalyticAgent.loadRequest(request)

        # writer: where the data will be saved
        writer = codecs.open(filename, 'w', encoding='utf-8')

        # grab the paths here and pass as parameters (not strictly necessary)
        path = request.Paths[0]
        apikey = request.Keys[0]

        # direct any logging output to the screen if not in quiet mode
        if screenLogging:
            logger = [sys.stdout]
        else:
            logger = None


        # Tell the agent to run with a name of Agent and perform a
        # query of type REPORT.            
        n = agt.run(jobName="Agent", writer=writer, logger=logger,
                    allowZeros=allowZeros, path=path, apikey=apikey,
                    limit=1000, queryType=QueryType.REPORT)

        print(u"Report completed. " + unicode(n) + u" records collected.")
        sys.exit(0)
        
    except AnalyticServerError as e:
        print(u"Agent> failed due to server error:\n"
              + unicode(e) )
        writer.close()
        # clean up the incomplete outfile
        if os.path.isfile(filename):
            os.remove(filename)
        sys.exit(1)
        
    except ZeroResultsError as e:
        print(u"Agent> failed due to zero results error")
        outfile.close()                    
        # clean up the incomplete outfile
        if os.path.isfile(filename):
            os.remove(filename)
        sys.exit(1)
        

if __name__ == "__main__":
    main(sys.argv)
