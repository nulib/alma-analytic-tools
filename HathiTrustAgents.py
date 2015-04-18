# HathiTrustAgents.py
# -*- coding: utf-8 -*-

from AnalyticAgent import AnalyticAgent

import codecs
import re
from string import rjust

class BaseHathiAgent(AnalyticAgent):
    PRINT_TITLES = False    
    def __init__(self):
        AnalyticAgent.__init__(self)
        self.noOCLC = []
        self.StillLoadingSleep = 10
        self.SleepNoise = 4
        self.noOCLCfilename = None        

    @staticmethod
    def data_filename(stem,extension=u'tsv',id=None,digits=None,leading='0'):
        """
        Extension of Analytic Agent's data_filename(...) that insists upon
        the extension being tsv.
        """
        return AnalyticAgent.data_filename(stem,u'tsv',id,digits,leading)
            
    def pre_process(self):
        self.noOCLC = []
        self.noOCLCfilename = "no-oclc-" + self._writer.name        

    def row_process(self, data):
         oclc = self.extractOCLC(data.get("Raw_OCLC", ""))
         if oclc is None:
             tup = ( data.get(self.Request.uniqueID), data.get("Title") )
             self.noOCLC.append(tup)
             return False
         else:
             data["OCLC"] = oclc

         if data.get("Raw_ISSN", None) is not None:
             data["ISSN"] = self.formatISSN(data.get("Raw_ISSN"))

         self._writer.write(self.hathiPrint(data, BaseHathiAgent.PRINT_TITLES) + u'\n')
         return True

    def post_process(self):
        # write the unique IDs of items missing OCLC numbers
        file = codecs.open(self.noOCLCfilename, 'w', encoding='utf-8')
        file.write( unicode(self.Request.uniqueID) + u"\t"
                    + u"Title" + u"\n")
        # print out noOCLC list        
        for id, title in self.noOCLC:
            file.write(unicode(id) + u"\t" + unicode(title) + u"\n")
        file.close()
        self.noOCLCfilename = None   
        
    def hathiPrint(self, data, printTitle):
        return "NOT IMPLEMENTED!!!\n"

    def extractOCLC(self, raw_oclc):
        """
        Attempt to get the first "best" OCLC. Best is defined as the
        first number that has the (OCoLC) leader and just the numbers.
        """
        oclc = None
        if raw_oclc is None:
            return None

        for tok in raw_oclc.split(";"):
            for t in tok.split(" "):
                m = re.match("\(OCoLC\)[0-9]+", t)
                if m is not None:
                    # found the answer
                    parts = m.group(0).partition(")")
                    oclc = parts[0] + parts[1] + parts[2].zfill(8)
                    return oclc
        # if we reach here, we've screwed up or no OCLC
        return oclc

    def formatISSN(self, raw_issn):
        # replace ; with commas
        issn = raw_issn.replace(";", ",")
        # remove internal spaces
        issn = issn.replace(" ", "")
        return issn

        
class HathiSerialAgent(BaseHathiAgent):
    def __init__(self):
        BaseHathiAgent.__init__(self)

    def hathiPrint(self, data, printTitle):
        oclc = data.get("OCLC")
        issn = data.get("ISSN", "")
        localid = data.get("MMS_ID")
        title = data.get("Title")
        govid = "" # we don't track this so it's always empty

        items = [ oclc, localid, issn, govid ]
        if printTitle:
            items.append(title)

        return  u'\t'.join(items)

class HathiMPMAgent(BaseHathiAgent):
    def __init__(self):
        BaseHathiAgent.__init__(self)

    def hathiPrint(self, data, printTitle):
        title = data.get("Title", "")
        oclc = data.get("OCLC", "")
        localid = data.get("Holding_ID") + u"/" + data.get("MMS_ID")
        status = "" # we drop missing/lost eventually, so skip this
        condition = "" # we don't track this one so it's always empty
        chronology = data.get("Summary_Holding", "")
        govid = "" # we don't track this so it's always empty

        items = [ oclc, localid, status, condition, chronology, govid ]
        if printTitle:
            items.append(title)

        return  u'\t'.join(items)

class HathiSPMAgent(BaseHathiAgent):
    def __init__(self):
        BaseHathiAgent.__init__(self)

    def hathiPrint(self, data, printTitle):
        title = data.get("Title", "")
        oclc = data.get("OCLC", "")
        localid = data.get("Item_ID") + \
                  u"/" + data.get("Holding_ID") + \
                  u"/" + data.get("MMS_ID")
        status = "" # we drop missing/lost eventually, so skip this
        condition = "" # we don't track this one so it's always empty
        govid = "" # we don't track this so it's always empty

        items = [ oclc, localid, status, condition, govid ]
        if printTitle:
            items.append(title)

        return  u'\t'.join(items)
