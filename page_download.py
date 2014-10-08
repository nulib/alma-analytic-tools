#!/usr/bin/env python
"""
Retrieve a "page" of results from an Alma analytic

This file is an example of how to perform a page-only query by using the
helper classes RequestObject and AnalyticAgent.

This script provides an interactive means to return a page of results
(25-1000 rows of data) from an Alma analytic using the RESTful API. The
user must enter the following data via terminal (or via a pipe or file
redirect):
  - Alma resource URL
  - The relative path to the analytic
  - An apikey
  - The number of results to return
  - The filename to save the results in
"""

from __future__ import print_function

import codecs
import math
import os
import stat
import sys

from AnalyticAgent import AnalyticAgent, QueryType
from RequestObject import RequestObject
from CustomExceptions import AnalyticServerError, ZeroResultsError

def print_help():
    """Print the help message."""
    print(u"Usage: page_download.py")
#end print_help()

def main(argv):
    """Main method that takes in user input, creates a RequestObject, and
    then creates/calls the AnalyticAgent to retrieve and save the page of
    data from thre report. 
    """

    # grab info to see how the input is coming in. If by pipe or file
    # redirect, we will echo the inputted values to the screen
    mode = os.fstat(0).st_mode
    echoInput = stat.S_ISFIFO(mode) or stat.S_ISREG(mode)
    
    if len(argv) != 1: # command
        print_help()
        sys.exit(2)

    print(u"Grab a Page of Data from an Analytic Report\n")

    # input the parameters
    url = raw_input(u"Alma resource URL:\n> ").strip()
    if echoInput: print(url)

    path = raw_input(u"Analytic path:\n> ").strip()
    if echoInput: print(path)

    apikey = raw_input(u"API key:\n> ").strip()
    if echoInput: print(apikey)

    limit = 0
    while not (AnalyticAgent.LIMIT_LOWER <= limit <= AnalyticAgent.LIMIT_UPPER):
        limit = raw_input(u"Number of results to return (25 - 1000):\n> ").strip()
        if echoInput: print(limit)
        limit = int(limit)
    # round up to nearest multiple of 25
    old_limit = limit
    limit = int(math.ceil(limit/25.0)) * 25
    if limit != old_limit:
        print("Limit rounded up to nearest multiple of 25: %d --> %d" \
              % (old_limit, limit))
              


    filename = raw_input(u"Save data to file:\n> ")
    if echoInput: print(filename)

    # create a simple Request
    request = RequestObject()
    request.Simple = True
    request.URL = url
    request.Paths.append(path)
    request.Keys.append(apikey)

    # unneeded check, but just to show that this is enough for a simple request
    assert request.validate(simpleRequest=True)

    # Create and run the agent
    print()
    print(u"Grabbing Analytic data...")
    try:
        agt = AnalyticAgent.loadRequest(request)

        # writer: where the data will be saved
        writer = codecs.open(filename, 'w', encoding='utf-8')

        # output any progress/log messages to standard out
        logger = [sys.stdout]

        # Tell the agent to run with a name of Agent and perform a
        # query of type PAGE.
        # Note that we leave out the path and apikey parameters since
        # the agent will pull them from the request object.
        n = agt.run(jobName="Agent", writer=writer, logger=logger,
                    limit=limit, queryType=QueryType.PAGE)

        print(u"Page collected. " + unicode(n) + u" records collected.")
        sys.exit(0)
        
    except AnalyticServerError as e:
        # Exception if the query is malformed or server cannot be reached
        print(u"Agent> failed due to server error:\n"
              + unicode(e) )
        writer.close()
        # clean up the incomplete outfile
        if os.path.isfile(filename):
            os.remove(filename)
        sys.exit(1)
        
    except ZeroResultsError as e:
        # See CustomExceptions for description of this error
        print(u"Agent> failed due to zero results error" )
        outfile.close()                    
        # clean up the incomplete outfile
        if os.path.isfile(filename):
            os.remove(filename)
        sys.exit(1)
        

if __name__ == "__main__":
    main(sys.argv)
