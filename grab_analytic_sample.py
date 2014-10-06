#!/usr/bin/env python

from __future__ import print_function

import codecs, getopt, os, sys

from SimpleAnalyticAgent import SimpleAnalyticAgent
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
    print(u"Usage: simple_analytic_query.py [options] <requestfile>")
    
    print(u"Options:")

    _opts = options.keys()
    _opts.sort()
    # get the length of the longest key for formatting purposes
    n = len( max(_opts, key=len) )

    for o in _opts:
        print( u" " + o.ljust(n+2,u' ') + options.get(o) )
#end print_help()

def main(argv):
    """Main method that processes the command line, loads the
    request object, initializes the factory, and then starts
    it up.
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
    
    # attempt to load the request object
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

    filename = SimpleAnalyticAgent.data_filename(fileStem)
    print(u"Loaded analtic request: " + requestFile)
    print(u"Output files: " + filename)

    print()
    print(u"Grabbing Analytic data...")
    print() 
    try:
        agt = SimpleAnalyticAgent.loadRequest(request)
        writer = codecs.open(filename, 'w', encoding='utf-8')
        path = request.Paths[0]
        apikey = request.Keys[0]
        n = agt.run(writer=writer, screenLogging=screenLogging,
                    allowZeros=allowZeros, path=path, apikey=apikey)
        sys.exit(0)
        
    except AnalyticServerError as e:
        print(strftime("%H:%M:%S") + u" : Agent> failed due to server error:\n"
                           + unicode(e) )
        writer.close()
        # clean up the incomplete outfile
        if os.path.isfile(filename):
            os.remove(filename)
        sys.exit(1)
        
    except ZeroResultsError as e:
        print(strftime("%H:%M:%S") + u" : Agent> failed due to zero results error" )
        outfile.close()                    
        # clean up the incomplete outfile
        if os.path.isfile(filename):
            os.remove(filename)
        sys.exit(1)
        

if __name__ == "__main__":
    main(sys.argv)
