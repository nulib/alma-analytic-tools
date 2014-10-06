# ComplexAnalyticAgent.py
# -*- coding: utf-8 -*-
"""
Code for managing RESTful queries with the ALMA Analytic API. This
includes the AnalyticAgent object class and the QueryType enumeration.

QueryType
  An enumeration characterizing the three types of queries that
  AnalyticAgent can perform:
     PAGE        Return only a 'limit' number of results, where limit is 
                 a parameter in the query such that 25 <= limit <= 1000.
     REPORT      Return multiple pages up to reaching the upper bound of
                 AnalyticAgent.OBIEE_MAX (currently 65001) records.
     ALL         Return multiple reports such that all records are pulled
                 from the analytic. Requires a non-simple RequestObject
                 as the queries must be specifically structured in order
                 to bypass the hard-coded OBIEE_MAX limit

AnalyticAgent
  An object class that uses RequestObject and QueryType to perform
  queries. Importantly, this class provides basic functionality for
  performing queries but also provides entry points (key functions) 
  for further customization through class extensions.
"""

from __future__ import print_function

from enum import Enum
import bidict

import codecs # for unicode read/write
import datetime
import re
import string
import sys

from multiprocessing import Queue
try:
    from lxml import etree
except ImportError: # No lxml installed 
    import xml.etree.cElementTree as etree
from random import randint, uniform
from time import sleep, strftime
from urllib2 import Request, urlopen, HTTPError
from urllib import urlencode, quote_plus

from CustomExceptions import AnalyticServerError, ZeroResultsError
from RequestObject import RequestObject

class QueryType(Enum):
    """
    An enumeration characterizing the three types of queries that AnalyticAgent
    can perform:
      PAGE        Return only a 'limit' number of results, where limit is a
                 parameter in the query such that 25 <= limit <= 1000.
      REPORT      Return multiple pages up to reaching the upper bound of
                 AnalyticAgent.OBIEE_MAX (currently 65001) records.
      ALL         Return multiple reports such that all records are pulled
                 from the analytic. Requires a non-simple RequestObject
                 as the queries must be specifically structured in order
                 to bypass the hard-coded OBIEE_MAX limit
    
    Enumeration characterizing the three types of queries that AnalyticAgent
    can perform:
    """
    PAGE = 1
    REPORT = 2
    ALL = 3
#end class QueryType
    
class AnalyticAgent(object):
    """
    Class object that performs a RESTful GET request to the Analytic API
    according to a provided RequestObject. This class presents a base
    class from which Analytic queries can be made. In particular, several
    methods are provided as hooks to allow future customization of the
    class through inheritance. These hooks primarily influence how data
    is transformed and outputted into its final format.

    Class Attributes:
    OBIEE_MAX
    The maximum number of requests that the OBIEE behind the Analytic
    will contain. Typically hardset by the database manager.

    LIMIT_LOWER, LIMIT_UPPER
    Integer values describing the lower and upper bounds (inclusive) for
    the 'limit' parameter in the RESTful GET request. 

    Object Attributes:
    Request
    The RequestObject containing the information for the query. This
    agent requires that Request.Simple be False.

    FailedRequestTolerance, ZeroResultTolerance
    A request can fail in two ways: returning a server error (Failed...)
    or by returning zero results when there are results to find
    (Zero...). The latter is a current bug in the Alma API. These two
    tolerance values indicate how many times the agent will sent requests
    before giving up when faced with these problems.

    ErrorSleep
    The number of seconds the agent should wait before resending a
    request after experiencing one of the aforementioned errors.

    StillLoadingSleep
    The number of seconds the agent waits before sending another request
    if the analytic is still loading the data.

    SleepNoise
    Adds a little random noise to the time the agent sleeps. Useful for
    preventing conflicts when running parallel agents.

    Protected Object Attributes
    The following are attributes used during queries by the agent for
    tracking and coordinating the progress of the agent. 
    
    _SeenIDs              A dictionary that keeps track of what uniqueIDs
                          have been seen by the agent in the returned XML.
                          This is used only when working around the
                          OBIEE_MAX  return limit.
    _jobName              String ID for the Agent
    _writer               File object for writing the collected data
    _logger               List of file or Queue objects for conveying
                          progress messages / logging info
    _allowZeros           Boolean indicating if an analytic query can
                          legitimately contain zero results
    _path                 Path to the analytic currently being queried
    _apikey               API key currently being used for the query
    _limit                Current number of records to be returned by a
                          call to _query_page() 
    _isFinished           Boolean indicating that _query_page() can return
                          more rows
    _resumeToken          Identifier for continuing a sequence of
                          _query_page() calls in order to download an
                          entire report
    _filterStart          The lower (>=) bound if filtering is being used
    _filterStop           The upper (<) bound if filtering is being used
    _filterStopReached    Boolean indicating if _filterStop has been
                          reached (essentially _filterValue >= _filterStop)
    _filterValue          Most recently collected datum from the record
                          field (sortedBy) being  used for filtering
    _page_RowCount        Number of rows processed by the most recent
                          call to _query_page()
    _page_RecordCount     Number of records collected by the most recent
                          call to _query_page
    _report_RowCount      Number of rows processed by the most recent call
                          to _query_record () 
    _report_RecordCount   Number of records collected by the most recent
                          call to _query_record()
    _all_RowCount         Number of rows processed by the most recent call
                          to _query_all()
    _all_RecordCount      Number of records collected by the most recent
                          call to _query_all()

    
    """

    # The maximum number of requests that the OBIEE behind the Analytic
    # will contain. Typically hardset by the database manager.
    OBIEE_MAX = 65001

    # Bounds (inclusive) for the limit parameter
    LIMIT_LOWER, LIMIT_UPPER = 25, 1000


    def __init__(self):
        """
        Basic constructor for initializing the AnalyticAgent.
        """
        self.Request = None
        self.FailedRequestTolerance = 3
        self.ZeroResultTolerance = 4
        self.ErrorSleep = 15
        self.StillLoadingSleep = 5
        self.SleepNoise = 2

        # internal variables used when conducting queries
        self._SeenIDs = {}
        self._jobName = ""
        self._writer = None
        self._logger = None
        self._path = None
        self._apikey = None
        self._limit = '1000'
        self._page_RowCount = 0
        self._page_RecordCount = 0
        self._report_RowCount = 0
        self._report_RecordCount = 0
        self._all_RowCount = 0
        self._all_RecordCount = 0        
        self._isFinished = False
        self._resumeToken = None
        self._filterStart = None
        self._filterStop = None
        self._filterStopReached = False
        self._filterValue = None
        self._allowZeros = False
    # end init

    @classmethod
    def loadRequest(cls,req):
        """
        Helper class method for creating an AnalyticAgent from a provided
        RequestObject.
        """
        agt = cls()
        agt.Request = req

        return agt
    #end load request

    @staticmethod
    def data_filename(stem,id=None):
        """
        HOOK: Static method that returns a filename for the type of data
        that this agent will produce.

        As this Agent currently outputs xml, it generates filenames of
        the form: stem[-id].xml

        Parameters:
          stem  The filestem to be used
          id    An identification number/symbol

        Returns:
          If id is None: stem.xml
          Else:          stem-id.xml
             
        """
        if id is None:
            return stem + u'.xml'
        else:
            return stem + u'-' + unicode(id) + u'.xml'

    def _increment_RowCounts(self):
        """
        Protected method for incrementing all RowCounts simultaneously.
        """
        self._page_RowCount += 1
        self._report_RowCount += 1
        self._all_RowCount += 1

    def _increment_RecordCounts(self):
        """
        Protected method for incrementing all RecordCounts simultaneously.
        """        
        self._page_RecordCount += 1
        self._report_RecordCount += 1
        self._all_RecordCount += 1    

    def _reset_internal_query_vars(self):
        """
        Protected method for resetting all protected attributes to the
        same default values as when an Agent is created by the
        constructor.
        """
        self._jobName = ""
        self._writer = None
        self._logger = None
        self._path = None
        self._apikey = None
        self._limit = '1000'
        self._page_RowCount = 0
        self._page_RecordCount = 0
        self._report_RowCount = 0
        self._report_RecordCount = 0
        self._all_RowCount = 0
        self._all_RecordCount = 0        
        self._isFinished = False
        self._resumeToken = None
        self._filterStart = None
        self._filterStop = None
        self._filterStopReached = False
        self._filterValue = None
        self._allowZeros = False
        self._SeenIDs = []

    def isLessThanFilterStop(self, textValue, filterStop):
        """
        Performs a proper less than comparison between textValue and
        filterStop by first converting textValue to Request.sortedByType.
        Note that all data returned by Analytic API are originally in
        text form.

        Currently provides support for decimal and string types. date and
        datetime are to be implemented at a later date.

        Parameters:
          textValue   The text of the sortedBy field in the current
                      row of data being queried
          filterStop  The sentinel value (already typed) that is never
                      to be exceeded by the query
                      
        Returns:
           If filterStop is None: True
           Else: returns appropriately typed (textValue < filterStop)
        """
        
        if filterStop is None:
            return True

        if self.Request.sortedByType == "string":
            return (textValue < filterStop)
        elif self.Request.sortedByType == "decimal":
            return (float(textValue) < filterStop)
        #for date
        #for dateTime = datetime.datetime.strptime(t,"%m/%d/%Y %I:%M:%S %p")
    #end isLessThan...

    def pre_process(self):
        """
        HOOK: This method is called by self.run(...) before sending any
        queries. It should contain functionality that prepares (if
        necessary) any structure for the output (which is typically sent
        to self._writer).

        Note that extensions of this and other process() methods can add
        attributes/variables to the AnalyticAgent class. 
                
        This base version of pre_process() create an XML structure (root
        is stored in self.outXML) into which the analytic data will be
        inserted. Included in this structure is the <Description> section
        that records information about this current analytic download.
        """
        self.outXML = etree.Element(u'Analytic')
        self.outXML.set(u'encoding','UTF-8')

        about = etree.SubElement(self.outXML, u'Description')

        node = etree.SubElement(about, u'Path')
        node.text = self._path
        
        node = etree.SubElement(about, u'Apikey')
        node.text = self._apikey

        if not self.Request.Simple:
            node = etree.SubElement(about, u'UniqueField')
            node.text = self.Request.uniqueID
        
            node = etree.SubElement(about, u'SortField')
            node.text = self.Request.sortedBy
            node.set(u'datatype', self.Request.sortedByType)
            node.set(u'obiee_field', self.Request.sortedByOBIEE)

            if self._filterStart is not None:
                node = etree.SubElement(about, u'GreaterOrEqual')
                node.text = self._filterStart            

            if self._filterStop is not None:
                node = etree.SubElement(about, u'LessThan')
                node.text = self._filterStop
        #end if complex request

        node = etree.SubElement(about, u'Started')
        node.text = strftime(u'%H:%M:%S %Z %Y-%m-%d')

        etree.SubElement(self.outXML, u'Results')
    #end pre_process

    def row_process(self, data):
        """
        HOOK: This method is called by self.run(...) for each row of
        data  in the analytic. This method is where the row data can
        be transformed and inserted into the output.

        The data parameter is a dictionary in which the keys are the
        preferred names from Request.ColumnMap or 'Column#'.

        Since the row data is only processed and outputted here,
        further filtering of the data can be conducted in this method.
        To ensure that record counts are accurate, this method should
        return a boolean value to indicate if the data was included in
        the output.

        This base version of row_process() creates an XML <Record>
        element, fills it with sub-elements based on what is in data,
        and inserts it into self.outXML's <Results> sub-element.
        """        
        results = self.outXML.find(u'Results')
        record = etree.SubElement(results,u'Record')

        if len(self.Request.NamesOrder) > 0:
            for _name in self.Request.NamesOrder:
                etree.SubElement(record, _name).text = unicode(data.get(_name, None))
        else:
            keys = data.keys()
            keys.sort()
            for k in keys:
                etree.SubElement(record, k).text = unicode(data.get(k))
        
        return True
        
    def post_process(self):
        """
        HOOK: This method is called by self.run(...) after the query has
        completed. This method is useful for any final edits, summaries,
        outputting, etc. of the collected data.

        This base method adds the completion time to the <Description>
        element in self.outXML and then sends the string version of
        the XML to self._writer.
        """
        about = self.outXML.find(u'Description')
        etree.SubElement(about, u'Completed').text = strftime(u'%H:%M:%S %Z %Y-%m-%d') 
        etree.SubElement(about, u'RecordCount').text = unicode(self._all_RecordCount)

        self._writer.write(etree.tostring(self.outXML, pretty_print=True))
  
    def log(self, msg):
        """
        Helper function for pushing logging messages to the correct place.
        If self._logger is None, no logging occurs and msg is discarded.
        Otherwise, it attempts to push the message out to what is in
        self._logger.

        For each message, the current time (HH:MM:SS) is prepended.

        Parameter:
          msg  String to be added to logs in self._logger
        """
        if self._logger is None:
            return

        msg = strftime("%H:%M:%S") + " : " + msg.strip()

        if not isinstance(self._logger, list):
            raise TypeError(u"AnalyticAgent.log(): self._logger must be a list object")

        for out in self._logger:
            if hasattr(out, "put"):
                out.put(msg)
            elif hasattr(out, "write"):
                out.write(msg + u"\n")
            else:
                raise TypeError(u"AnalyticAgent: self._logger element: '" + unicode(out)
                                + "u' does not have a write(...) or put(...)method" )            
        #end for out in ...
    #end log(...)

    def _duplicate_check(self, newID):
        """
        Helper method for determining if newID has already been processed
        by this analytic by using self._SeenIDs.

        Parameter:
          newID   The value of uniqueID from a row of data

        Returns:
          True:   If self.seenIDs[newID] exists
          False:  If self.seenIDs[newID] does not
          
        """
        if self._SeenIDs is None:
            self._SeenIDs = {}
        
        if newID in self._SeenIDs:
            self._SeenIDs[newID] += 1
            return True
        else:
            self._SeenIDs[newID] = 1
            return False
    #end duplicate_check

    def noisy_sleep(self, t, delta):
        """
        Call for this thread to sleep for a random number of seconds
        from the distribution [t-delta, t+delta].

        Parameters:
          t      A positive number of seconds to sleep
          delta  An error range (0 <= delta <= t)

        Throws:
          ValueError on incompatible parameters
          
        """
        if t <= 0:
            raise ValueError(u"noisy_sleep: t must be positive")
        if delta < 0:
            raise ValueError(u"noisy_sleep: delta must be non-negative")
        if delta > t:
            raise ValueError(u"noisy_sleep: delta must be <= t")
            
        sleep( t + uniform(-delta,delta) )
    #end noisy_sleep
        
    def generate_request(self, path='', apikey='', limit='1000', 
                       resume=None, filter=None):
        """
        Function that generates (but does not call) and %-encodes
        the HTTP GET request to the Analytic ALMA API using data
        from self.Request and the parameters.

        Parameters:
          path      The relative path to the analytic to be queried
          apikey    The apikey to use in the query
          limit     The number of results (25 - 1000) to be returned
          resume    A resumption token (optional)
          filter    A filter statement (optional)

        Returns:
          A properly percent-encoded / utf-encoded urllib2 Request
          object ready for sending
        """
        params = {}
        params['path'] = path.encode('utf-8')
        params['apikey'] = apikey.encode('utf-8')
        params['limit'] = limit.encode('utf-8')

        if filter is not None:
            params['filter'] = filter.encode('utf-8')

        if resume is not None:
            params['token'] = resume.encode('utf-8')
            
        req = Request( self.Request.URL + '?' + urlencode(params) )
        req.get_method = lambda: 'GET'

        return req
    #end generate_request

    def create_filter(self, tag):
        """
        Creates a sawx filter for the analytic to return values from
        the self.Request.sortedBy field of type self.Request.sortedByType
        such that the values are greater than or equal to the value in
        tag.

        Notes:
          To avoid processing issues, any apostrophes in tag are
          replaced with &apos;.

          As only decimal and string types are currently supported,
          there is no need to transform tag into a proper format.
          Supporting date and datetime types may require some
          transformation.

        Parameters:
          tag  The text that needs to be inserted into the filter

        Returns:
          A sawx filter for self.Request.sortedBy field for records
          greater than or equal to tag
        """
        filter = u'<sawx:expr xsi:type="sawx:comparison" op="greaterOrEqual" xmlns:saw="com.siebel.analytics.web/report/v1.1" xmlns:sawx="com.siebel.analytics.web/expression/v1.1" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema"><sawx:expr xsi:type="sawx:sqlExpression">"#FIELD#"</sawx:expr><sawx:expr xsi:type="xsd:#TYPE#">#TAG#</sawx:expr></sawx:expr>'

        filter = filter.encode('utf-8')

        filterField = self.Request.sortedByOBIEE
        filterField = u'"."'.join(filterField.split(u'.')).encode('utf-8')

        filterType = self.Request.sortedByType.encode('utf-8')

        tag = unicode(tag)
        cleanTag = tag.replace(u"'",u"&apos;")

        filter = filter.replace(u'#FIELD#', filterField)
        filter = filter.replace(u'#TYPE#', filterType)
        filter = filter.replace(u'#TAG#', cleanTag)

        return filter
    #end create_filter

    def run(self, jobName="", writer=sys.stdout, logger=None, allowZeros=False,
            path=None, apikey=None, limit='1000',
            filterStart=None, filterStop=None, queryType=QueryType.ALL):
        """
        This is the function that is used to make the Agent run an
        Analytic Query. It initializes / clears out the protected internal
        query attributes and coordinates the calling of the pre_process,
        _query_*, and post_process methods. 

        Parameters:
          jobName      String ID for the Agent
          writer       File object for writing the collected data
          logger       List of file or Queue objects for conveying
                       progress messages / logging info
          allowZeros   Boolean indicating if an analytic query can
                       legitimately contain zero results
          path         Path to the analytic currently being queried.
                       Defaults to self.Request.Paths[0] if omitted.
          apikey       API key currently being used for the query.
                       Defaults to self.Request.Keys[0] if omitted.
          limit        Number of records to be returned by a single
                       call to _query_page() 
          filterStart  The lower (>=) bound if filtering is being used
          filterStop   The upper (<) bound if filtering is being used
          queryType    Value from enum QueryType indicating what type
                       of query to perform (ALL, REPORT, PAGE).

        Returns:
          The number of collected records from this call to run()

        Throws:
          ValueError if parameters or the agent's RequestObject are
          invalid for the type of call to run
                       
        """
        # do basic parameter checking first
        if queryType not in QueryType:
            raise ValueError(u"Unrecognized QueryType passed into queryType parameter.")
        
        if queryType is QueryType.ALL and self.Request.Simple:
            raise ValueError(u"query_all() requires the agent to have a non-simple RequestObject")

        if not (AnalyticAgent.LIMIT_LOWER <= int(limit) <= AnalyticAgent.LIMIT_UPPER):
            raise ValueError(u"limit must in the range ["
                             + unicode(AnalyticAgent.LIMIT_LOWER)
                             + u", "
                             + unicode(AnalyticAgent.LIMIT_UPPER)
                             + u"]")


        # fill in path and apikey if not provided
        if path is None:
            path = self.Request.Paths[0]
        if apikey is None:
            apikey = self.Request.Keys[0]


        # initialize the internal query vars
        self._jobName = jobName
        self._writer = writer
        self._logger = logger
        self._path = path
        self._apikey = apikey
        self._resumeToken = None
        self._limit = unicode(limit)
        self._filterStart = filterStart
        self._filterStop = filterStop
        self._allowZeros = allowZeros
        
        self._page_RowCount = 0
        self._page_RecordCount = 0
        self._report_RowCount = 0
        self._report_RecordCount = 0
        self._all_RowCount = 0
        self._all_RecordCount = 0        

        self._SeenIDs = {} 
        self._isFinished = False
        self._filterStopReached = False
        self._filterValue = None    


        # run initial processing step
        self.pre_process()
        
        # run the appropriate query call
        if queryType is QueryType.ALL:
            self._query_all()
            totalRecordCount = self._all_RecordCount
            
        elif queryType is QueryType.REPORT:
            self._query_report()
            totalRecordCount = self._report_RecordCount

            if self._report_RowCount == AnalyticAgent.OBIEE_MAX:
                print(u"\nWARNING: Analytic may contain more data.\n",
                      file=sys.stderr)
                
        elif queryType is QueryType.PAGE:
            self._query_page()
            totalRecordCount = self._page_RecordCount
            if not self._isFinished and not self._filterStopReached:
                print(u"\nWARNING!!! Analytic contains more data.\n",
                      file=sys.stderr)

        # wrap up the processing
        self.post_process()

        # reset the internal query vars
        self._reset_internal_query_vars()

        # return the number of records found
        return totalRecordCount
    #end run(...)

    def _query_all(self):
        """
        Internal method for downloading all results from an analytic.
        Coordinates multiple calls to _query_report() using the
        protected, internal query attributes.
        """
        
        if self.Request.Simple:
            raise ValueError(u"query_all() requires the agent to have a non-simple RequestObject")

        self._all_RowCount = 0
        self._all_RecordCount = 0

        moreRecords = True        
        while moreRecords:
            self._query_report()

            # we need to change the filterStart as the next report needs to start
            # where the previous one left off
            self._filterStart = self._filterValue
            
            moreRecords = (not self._filterStopReached) and \
                          (self._report_RowCount == AnalyticAgent.OBIEE_MAX)
        #end while moreRecords

        self.log(self._jobName
                 + u"> has finished collecting all "
                 + unicode(self._all_RecordCount)
                 + u" record(s)")

        # adjust values of receipt to return
            
        return 
    #end query_all

    def _query_report(self):
        """
        Internal method for downloading a report (OBIEE_MAX rows) from
        an analytic. Coordinates multiple calls to _query_page() using
        the protected, internal query attributes.
        """        
        self._report_RowCount = 0
        self._report_RecordCount = 0
        
        self._isFinished = False
        
        while not self._isFinished:
            self._query_page()
        #end while not isFinished

        self.log(self._jobName
                 + u"> collected a report of "
                 + unicode(self._report_RecordCount)
                 + u" record(s) ("
                 + unicode(self._all_RecordCount)
                 + u" total)")

        # reset resumeToken to None as we just exhausted it
        self._resumeToken = None
        
        return
    #end query_report
    
    def _query_page(self):
        """
        Internal method for downloading a page of results (self._limit)
        by using the protected, internal query attributes. This is the
        only method that actually performs HTTP GET requests.

        Throws:
          AnalyticServerError and ZeroResultsError

        """        
        self._page_RowCount = 0
        self._page_RecordCount = 0

        self._filterValue = self._filterStart
        filterParam = None

        # put in a filter if the request if not simple
        if not self.Request.Simple and self._filterValue is not None:
            filterParam = unicode(self.create_filter(self._filterValue))

        row_namespace = None
        self._isFinished = False
        zeroTries = 0

        # keep cycling until we either get data (triggers a break)
        # or we throw either AnalyticServerException or ZeroResultsException
        while True:
            _request = self.generate_request(path=self._path,
                                             apikey=self._apikey,
                                             limit=self._limit,
                                             resume=self._resumeToken,
                                             filter=filterParam)
            
            # keep trying until we get a valid HTTP response
            requestTries = 0
            loopFlag = True
            while loopFlag:
                try: # hopefully we won't get a server error
                    requestTries = requestTries + 1
                    response = urlopen(_request).read()
                    loopFlag = False
                except HTTPError as e:
                    # if we do, sleep and try again until tolerance exceeded
                    if requestTries < self.FailedRequestTolerance:
                        loopFlag = True
                        self.log(self._jobName
                                 + u" received error status " \
                                 + unicode(e.code) + u", trying again...")
                        self.noisy_sleep(self.ErrorSleep, self.SleepNoise)
                    else:
                        # throw a custom exception with the HTTPError
                        msg = u"Error Code: " + unicode(e.code) + u"\n"
                        msg = msg + u"Returned data:" + u"\n"
                        msg = msg + unicode(e.read()) + u"\n"
                        raise AnalyticServerError(msg)
                #end try/except
            #end while loopFlag

            # If here, HTTP Status is 200 and xml has been returned
            
            data_xml = etree.fromstring(response)

            token = data_xml.findtext(".//ResumptionToken")
            if token is not None:
                # grab the ResumptionToken that you will use from now on
                self._resumeToken = token 

            # sometimes, while the analytic is loading, it returns empty results
            # in that there is nothing in the ResultXml tag, not even the xsd info
            # solution: sleep for a bit and try again using the resume token
            if (len(data_xml.find(".//ResultXml")) == 0):
                self.log(self._jobName + u"> Analytic is still loading...")
                self.noisy_sleep(self.StillLoadingSleep, self.SleepNoise)
                continue

            # grab the namespace of the rowset 
            if row_namespace is None:                    
                row_namespace = '{' + data_xml.find(".//ResultXml")[0].nsmap[None] + '}'

            # check for the zero results but is finished bug
            if not self._allowZeros and len(data_xml.findall(".//" + row_namespace + "Row")) == 0:
                self._resumeToken = None
                zeroTries = zeroTries + 1
                if zeroTries < self.ZeroResultTolerance:
                    self.log(self._jobName
                             + u"> Analytic returned zero results. Trying again...")
                    self.noisy_sleep(self.ErrorSleep, self.SleepNoise)
                else:
                    raise ZeroResultsError()
            else:
                # we have data, so break
                break
        #end while true

        # grab the isFinished flag as part of the return values
        self._isFinished = (data_xml.findtext(".//IsFinished").lower() == 'true')

        self._filterStopReached = False
        # iterate over each row
        for row in data_xml.findall(".//" + row_namespace + "Row"):
            self._increment_RowCounts()

            # load the data from each column and save into a dictionary
            row_data = {}
                        
            if len(self.Request.NamesOrder) > 0:
                # columnmap exists
                for _name in self.Request.NamesOrder:
                    _column = self.Request.ColumnMap.names.get(_name)
                    _value = row.findtext(row_namespace + _column, default=None)
                    row_data[_name] = _value
            else:
                # grab all columns save 0
                for _col in row:
                    _tag =  re.search("Column[0-9]+",_col.tag).group(0)                        
                    if _tag == "Column0":
                        continue
                    row_data[_tag] = _col.text                       

            if not self.Request.Simple:
                # grab the next filterValue
                self._filterValue = row_data.get(self.Request.sortedBy)
                
                if not self.isLessThanFilterStop(self._filterValue, self._filterStop):
                    self._isFinished = True # do not repeat a query with the token
                    self._filterStopReached = True
                    break # end 'for row in data_xml...'

                # check for duplicates
                if self._duplicate_check(row_data.get(self.Request.uniqueID)):
                    # duplicate found, so skip and continue
                    continue

            row_accepted = self.row_process(data=row_data)

            if row_accepted:
                self._increment_RecordCounts()
        #end for row in data_xml...

        self.log(self._jobName
                 + u"> collected a page of "
                 + unicode(self._page_RecordCount) + u" / "
                 + unicode(self._page_RowCount) + u" records ("
                 + unicode(self._all_RecordCount) + u" total)")

        return 
   #end query_page
#end class AnalyticAgent

