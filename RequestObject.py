# RequestObject.py
# -*- coding: utf-8 -*-
"""
Source code for the RequestObject class
"""

import bidict
import codecs 
import re

from random import shuffle

try:
    from lxml import etree
except ImportError: # No lxml installed 
    import xml.etree.cElementTree as etree

class RequestObject(object):
    """
    Class object containing the necessary information for making a request
    to download data from an analytic using the RESTful API.

    Attributes:
    Simple         Boolean indicating if the Request is simple (<=65001
                   rows, does not need uniqueID, no sortedBy*, no
                   multiprocessing, etc.).

    URL            The resource URL for the Alma API.

    Paths          A list of file paths to the analytic to download from. 
                   To better suppport parallel requests, it is recommended 
                   you create copies of the analytic and have separate 
                   threads access different copies.

    Keys           A list of apikeys for authentication with the Alma API. 
                   As with Paths, parallelism is best supported by using
                   separate REST apikeys for the Alma API.

    ColumnMap      A named bidirectional dictionary (bidict) for mapping 
                   the column order from the analytic XML to preferred XML
                   names (e.g., <Column1></Column1> --> <Title></Title>). 
                   The two mappings can be accessed directly as
                   ColumnMap.columns and ColumnMap.names, respectively.

    NamesOrder     A list containing the keys in ColumnMap.names in the
                   order they should appear in the output.

    uniqueID       The XML name from ColumnMap.names that acts as a unique
                   identifier for each entry in the analytic's results
                   table.
                   
    sortedBy       The XML name from ColumnMap.names that is used for
                   sorting the entries in the analytic's results table.
                   
    sortedByType   The OBIEE datatype (decimal, string, date, etc.) of
                   sortedBy.
                   
    sortedByOBIEE  The name of the OBIEE field.subfield that contains the 
                   data for sortedBy
                   
    JobBounds      A list of values used to distinguish the different jobs
                   that parallel code will perform by creating filters on 
                   the sortedBy field/column in the analytic. Each job is
                   defined by two consecutive bounds, meaning that for N
                   jobs, there will be N+1 total bounds. For a single job,
                   the bounds x and y will  make the Agent find only
                   results such that:
                                  { x <= row.sortedBy < y }
                   A None at the beginning or end of  the bounds list means
                   the lower (or upper) limit is unbounded. For example, if
                   JobBounds = [ None, 'H', 'P' ], the two jobs will be:
                    { row.sortedBy <'H' } and  { 'H' <= rw.sortedBy <'P' }
    """
    
    def __init__(self):
        """
        Basic constructor for initializing the RequestObject.
        """
        self.Simple = None
        self.URL = None
        self.Paths = []
        self.Keys = []
        self.uniqueID = None
        self.sortedBy = None
        self.sortedByType = None
        self.sortedByOBIEE = None
        self.JobBounds = [None, None]
        self.NamesOrder = []
        self.ColumnMap = bidict.namedbidict('biMap', 'columns', 'names')({})
    # end __init__

    @classmethod
    def fromFilename(cls, filename, simpleRequest=False):
        """Helper class method for creating a valid RequestObject from
        a filestream.

        See external documentation for specifications on the input format
        and what distinguishes simple versus complex request objects.
        
        Parameters:
          filename        A path to a file containing the request input
          simpleRequest   Boolean to indicate if the Request is to be
                          Simple or Complex
        Returns:
          RequestObject 
        Throws:
          Exceptions upon malformed or invalid input
        """
        ro = cls()
        ro._parse_input(codecs.open(filename, 'r', encoding='utf-8'),
                        simpleRequest=simpleRequest)
        return ro

    @classmethod
    def fromFilestream(cls, filestream, simpleRequest=False):
        """Helper class method for creating a valid RequestObject from
        a filestream.

        See external documentation for specifications on the input format
        and what distinguishes simple versus complex request objects.

        Parameters:
          filestream      A file object (with the read property activated)
          simpleRequest   Boolean to indicate if the Request is to be
                          Simple or Complex
        Returns:
          RequestObject 
        Throws:
          Exceptions upon malformed or invalid input
        """
        ro = cls()
        ro._parse_input(filestream, simpleRequest=simpleRequest)
        return ro
            
    def reset(self):
        """Helper method for emptying all data contained in the RequestObject.
        This RequestObject is now equivalent to a newly constructed one.
        """
        for x in self.__dict__.keys():
            setattr(self, x, None)
        self.Paths = []
        self.Keys = []
        self.JobBounds = [None,None]
        self.NamesOrder = []
        self.ColumnMap = bidict.namedbidict('biMap', 'columns', 'names')({})

    def _parse_input(self, reader, simpleRequest=False):
        """Private method that places data in the attributes from the file
        object parameter 'reader.' Once all data is loaded, a validation
        check is performed.

        May raise an exception if there are any syntax errors in the input
        dat or if the resulting RequestObject is not valid.

        Parameters:
          reader          A filestream (read) from which the RequestObject
                          will attempt to load its data
          simpleRequest   Boolean to indicate if the Request is to be
                          Simple or Complex
        Throws:
          Exceptions upon malformed input or validation failure 
        """
        self.reset()
        self.Simple = simpleRequest
        inColumnMap = False
        lineNo = 1
        for line in reader:
            lineNo = lineNo + 1
            line = line.lstrip()
            if line.startswith('#') or len(line.strip()) == 0:
                continue
            tokens = line.split('\t')
            tokens[0] = tokens[0].strip()

            if tokens[0] == 'url':
                if self.URL is not None:
                    self._parse_error(reader,lineNo,'Only one url allowed in a request')
                try:
                    self.URL = tokens[1].strip()
                except:
                    self._parse_error(reader,lineNo,'Problem extracting URL')
                        
            elif tokens[0] == 'path':
                try:
                    self.Paths.append(tokens[1].strip())
                except:
                    self._parse_error(reader,lineNo,'Problem extracting path')                    
                    raise Exception(reader.name + ': error on line ' \
                                    + unicode(lineNo))
            elif tokens[0] == 'apikey':
                try:
                    self.Keys.append(tokens[1].strip())
                except:
                    self._parse_error(reader,lineNo,'Problem extracting apikey.')                    

            elif tokens[0] == 'jobBounds' :
                if simpleRequest:
                    continue;
                if self.sortedByType is None:
                    self._parse_error(reader,lineNo,'job_bounds cannot appear before sortedBy in the input file')
                self.JobBounds = []
                try:
                    for bound in (line.split('\t',1)[1].split(',')):
                        bound = bound.strip()
                        if bound == '':
                            self.JobBounds.append(None)
                        elif self.sortedByType == 'decimal':
                            self.JobBounds.append(float(bound))
                        else:
                            self.JobBounds.append(bound)
                except:
                    self._parse_error(reader,lineNo,'Problem with reading bounds or type conversion')

            elif tokens[0] == 'uniqueID':
                if simpleRequest:
                    continue;                
                if self.uniqueID is not None:
                    self._parse_error(reader,lineNo,'One one uniqueID allowed')                    
                try:
                    self.uniqueID = tokens[1].strip()
                except:
                    self._parse_error(reader,lineNo,'Problem extracting unique ID')                    

            elif tokens[0] == 'sortedBy':
                if simpleRequest:
                    continue;                
                if self.sortedBy is not None:
                    self._parse_error(reader,lineNo,'One one sortedBy is allowed')                    
                try:
                    self.sortedBy = tokens[1].strip()
                except:
                    self._parse_error(reader,lineNo,'Problem extracting sortedBy name.')

            elif tokens[0] == 'sortedByType':
                if simpleRequest:
                    continue;
                
                if self.sortedByType is not None:
                    self._parse_error(reader,lineNo,'One one sortedByType is allowed')
                try:
                    self.sortedByType = tokens[1].strip()
                except:
                    self._parse_error(reader,lineNo,'Problem extracting sortedByType data.')

            elif tokens[0] == 'sortedByOBIEE':
                if simpleRequest:
                    continue;                
                if self.sortedByOBIEE is not None:
                    self._parse_error(reader,lineNo,'One one sortedByOBIEE is allowed')                    
                try:
                    self.sortedByOBIEE = tokens[1].strip()
                except:
                    self._parse_error(reader,lineNo,'Problem extracting sortedByOBIEE data.')
                    
            elif tokens[0] == 'Begin NamesToColumns':
                if len(self.ColumnMap) > 0 or inColumnMap:
                    self._parse_error(reader,lineNo,'Begin NamesToColumns can appear only once')
                inColumnMap = True

            elif tokens[0] == 'End NamesToColumns':
                if not inColumnMap:
                    self._parse_error(reader,lineNo,'Unmatched End NamesToColumns')
                inColumnMap = False
                
            elif inColumnMap:
                # read the next lines and put into the bidirectional dict
                try:
                    self.NamesOrder.append(tokens[0].strip())
                    self.ColumnMap[tokens[1].strip().capitalize()] = tokens[0].strip()
                except:
                    self._parse_error(reader,lineNo,'Problems reading column to name data')
            else:
                self._parse_error(reader,lineNo,'Unrecognized error')            

        # if jobbounds was empty, make it none,none
        if self.JobBounds is None:
            self.JobBounds = [None, None]

        # shuffle the Paths and Keys for the heck of it
        shuffle(self.Paths)
        shuffle(self.Keys)

        # end for line in reader
        errors = []
        if not self.validate(log=errors,simpleRequest=simpleRequest):
            msg = u"Error(s) found in this RequestObject's data:\n"
            for e in errors:
                msg = msg + unicode(e) + u"\n"
            raise Exception(msg)
        
    # end parse

    def _parse_error(self, reader, lineNo, msg):
        """
        Private helper function for raising exceptions and giving
        feedback when a parsing error occurs.

        Parameters:
          reader   Filestream being used by _parse_input(...)
          lineNo   The line number the error was encountered on
          msg      A string message describing the error

        Throws:
          Always throws an exception that includes the passed
          in information
        """
        raise Exception(reader.name + ', line ' + unicode(lineNo) \
                        + ': ' + msg)

    def validate(self,log=[],simpleRequest=None):
        """Helper method for checking that sufficient data is found within
        this RequestObject for performing a RESTful query as either a
        simple or complex request. For each error found, a text statement 
        is added to log to support debugging. As such, validity is
        equivalent to (len(log) == 0).

        Parameters:
          log            An empty list object into which debugging messages
                         will be appended.
                         NOTE: If log is not initially empty, validate 
                               will by default return False
          simpleRequest  Boolean to indicate if the Request is to be
                         considered as Simple or Complex for validation
                         purposes.If left blank, this object's Simple
                         attribute is used instead.
        Returns:
          True or False depending on if any errors were found (equivalent
          to len(log) == 0).
        """

        if simpleRequest is None and self.Simple is None:
            log.append('RequestObject.Simple is None. Must be set to a boolean value.')
            return False
        else:
            simpleReqest = self.Simple

        
        
        # must be a URL
        if self.URL is None or len(self.URL) == 0:
            log.append('Resource url (url) not found or is empty')

        # must be at least one path that is non-zero
        if len(self.Paths) == 0:
            log.append('No analytic paths (path)')
        for p in self.Paths:
            if len(p) == 0:
                log.append('Empty analytic path (path)')

        # must be at least one key that is non-zero
        if len(self.Keys) == 0:
            log.append('No api keys (apikeys)')
        for k in self.Keys:
            if len(k) == 0:
                log.append('Empty apikey (apikey)')        

        # columns to names
        if not simpleRequest and len(self.ColumnMap) == 0:
            log.append('Table is not described. NamesToColumns is empty')
        for col in self.ColumnMap.columns.keys():
            if len(col) == 0:
                log.append('Column names cannot be zero length')
            if re.match("^Column[0-9]+$",col) is None:
                log.append('Column name ' + col + ' is not of the form Column[0-9]+')
            xid = self.ColumnMap.columns[col]
            if len(xid) == 0:
                log.append('Preferred name for a column cannot be zero length')
            try: # see if xid will be a valid xml element name
               root = etree.Element(xid)
            except:
                log.append(xid + ': is not a valid xml element name')
        
        # uniqueID
        if simpleRequest:
            pass
        elif self.uniqueID is None or len(self.uniqueID) == 0:
            log.append('uniqueID not found or is empty')
        elif self.uniqueID not in self.ColumnMap.names.keys():
            log.append('uniqueID was not found in NamesToColumns')

        # sortedBy
        if simpleRequest:
            pass
        elif self.sortedBy is None or len(self.sortedBy) == 0:
            log.append('sortedBy not found or is empty')
        elif self.sortedBy not in self.ColumnMap.names.keys():
            log.append('sortedBy was not found in NamesToColumns')

        # sortedByType
        if simpleRequest:
            pass        
        elif self.sortedByType is None or len(self.sortedByType) == 0:
            log.append('The type for sortedBy was not found or is empty')
        elif self.sortedByType not in ['decimal', 'string']:
            log.append(self.sortedByType + ' is not a currently supported type')

        # sortedByOBIEE
        if simpleRequest:
            pass        
        elif self.sortedByOBIEE is None or len(self.sortedByOBIEE) == 0:
            log.append('sortedByOBIEE was not found or is empty')
        elif len(self.sortedByOBIEE.split('.')) != 2:
            log.append(self.sortedByOBIEE + ' is not of the form Field.Subfield\n or has extra periods in it.')


        # JobBounds: if exists, None only at start and end and JB[i] < JB[i+1]
        if simpleRequest:
            pass
        elif self.JobBounds is None:
            self.JobBounds = [None, None]
        elif len(self.JobBounds) == 1:
            log.append('job_bounds must contain at least 2 bounds.')
        else:
            for i in  xrange(0, len(self.JobBounds) - 1):
                if (self.JobBounds[i] is None) and (i != 0):
                    log.append('An infinity bound (empty bound) can occur only ' + \
                               'at the start or end of the list')
                elif (self.JobBounds[i] >= self.JobBounds[i+1]) and (self.JobBounds[i+1] is not None):
                    log.append('JobBounds[' + unicode(i) + '] is greater than or equal to ' \
                               + 'JobBounds[' + unicode(i+1) + ']: ' + unicode(self.JobBounds[i]) \
                               + ' >= ' + unicode(self.JobBounds[i+1]) \
                               )

        return (len(log) == 0)
    # end validate
# end class RequestObject       
