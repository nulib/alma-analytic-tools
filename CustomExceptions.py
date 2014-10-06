# CustomExceptions.py
"""A collection of simple extensions of the Exception class for use with
associated files:
   - RequestObject.py
   - AnalyticAgent.py
   - QueryFactory.py
   - The various example scripts (page_download.py, download_all.py, etc.)
"""

class DamnIt(Exception):
    """General purpose exasperation but one that can be deliberately
    thrown and readily identified.
    """
    pass

class AnalyticServerError(Exception):
    """Thrown by AnalyticAgent when making a query if an excessive number
    of errors are returned to the GET requests. Such errors occur for
    various reasons but mostly due to either:
      - The API server is down, busy, etc.
      - Parameters were malformed (e.g., limit > 1000)
    """
    pass

class ZeroResultsError(Exception):
    """
    Thrown by AnalyticAgent when making a query. This exception refers
    to a current bug in the Analytic API. Sometimes, an analytic will
    return zero results and claim that isFinished is True even though
    there is data in the analytic. This bug appears to occur if a
    particular analytic is queried too often (and maybe with the same
    apikey). Generally, this bug reqires a long wait time (i.e., 24+
    hours) to resolve, but may self-correct quickly. The proffered
    solution is to reset the query and try again after a short wait
    period.

    Unfortunately, this bug makes it impossible to programmatically
    distinguish between actual zero results and this anomaly.
    """
    pass
