# QueryFactory.py
# -*- coding: utf-8 -*-
"""
Various classes for running parallel queries to an Analytic based upon
jobs listed in a complex RequestObject. Generally only used for large
QueryType.ALL work.

Classes:
  QueryFactory
  This is the primary class for coordinating the parallel work performed
  by the other classes. 

  Worker
  This multiprocessing.Process class performs the creation and calling
  of AnalyticAgents to query and gather the data from an analytic. The
  QueryFactory creates multiple Workers to perform the various parallel
  jobs list in a RequestObject.

  Coordinator
  This multiprocessing.Process class oversees the Workers by keeping
  track of completed, failed, and abandoned jobs. The QueryFactory
  creates a single Coordinator that determines when the factory can
  be shut down.
  
  Logger
  This multiprocessing.Process handles the collection, recording, and
  reporting of any messages produced by Workers, their Agents, and
  the Coordinator.

  PersistentSyncManager
  This helper class is used to make it easier to cancel/quit a
  QueryFactory when it is running through keyboard interrupts
  (i.e., Ctrl+C).
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
from time import sleep, strftime
from random import uniform, shuffle
from math import ceil, log10

import multiprocessing
import multiprocessing.managers
import os
import codecs # for unicode read/write
import sys
import logging
import signal

from RequestObject import RequestObject
from AnalyticAgent import AnalyticAgent, QueryType
from CustomExceptions import DamnIt, AnalyticServerError, ZeroResultsError


class QueryFactory(object):
    """
    The QueryFactory class uses multiprocessing (independent threads that
    may run on different cores on the same machine) to coordinate and
    retrieve data from an analytic. This Factory is particularly designed
    for performing QueryType.ALL requests, particularly if said job is
    more efficiently broken into independent sub-tasks.
    
    Attributes:
    Request         A RequestObject detailing the information for the
                    query. The  factory requires a complex RequestObject.
    AgentClass      The class object representing the type of agent to
                    use for making the queries. This should be the
                    AnalyticAgent class or a subclass of it.
    NumWorkers      An integer indicating the number of workers (separate
                    agents) to be run in parallel.
    FileStem        The filestem by which all ouputted files will be
                    written.
    ToStandardOut   Boolean indicating if logging will be written to the
                    screen (logging will always be written to file).
    AllowZeros      Boolean indicating if the queries should accept
                    returns with zero results as correct.
    ResumeWork      Boolean indicating if the factory should look at
                    existing output files in an attempt to avoid redoing
                    the same subjobs.
    """
    
    def __init__(self):
        """Basic object constructor."""
        self.Request = None
        self.AgentClass = None
        self.NumWorkers = None
        self.FileStem = None
        self.ToStandardOut = None
        self.AllowZeros = None
        self.ResumeWork = None

    @classmethod
    def setup(cls, request, num_workers=1, file_stem='data',
              to_standard_out=True, allow_zeros=False,
              resume_work=False, agent_class=AnalyticAgent):
        """
        Class method for setting up a factory with the attributes
        accordingly initialized.

        Parameters:
          request           self.Request
          agent_class       self.AgentClass  
          num_workers       self.NumWorkers
          file_stem         self.FileStem
          to_standard_out   self.ToStandardOut
          allow_zeros       self.AllowZeros
          resume_work       self.ResumeWork

        Returns:
          An instantiated QueryFactory object

        Throws:
          ValueError if agent_class is not a subclass of AnalyticAgent or
          if the RequestObject is simple
        """
        if not issubclass(agent_class, AnalyticAgent):
            raise ValueError(u"agent_class needs to be a subclass of AnalyticAgent.")
        if request.Simple:
            raise ValueError(u"QueryFactory requires a non-simple RequestObject")
        
        factory = cls()
        factory.Request = request
        factory.NumWorkers = num_workers
        factory.FileStem = file_stem
        factory.ToStandardOut = to_standard_out
        factory.AllowZeros = allow_zeros
        factory.AgentClass = agent_class
        factory.ResumeWork = resume_work
        return factory

    def engage(self):
        """
        Make the factory begin querying the analytic as according to the
        internal attributes: RequestObject, NumWorkers, etc.

        Process:
          Generates the job list from the RequestObject.
          Writes the job mapping file.
          If self.ResumeWork:
            Runs through the current directory to see if any output
            files of the same format as this Factory's exist. Those
            jobs are removed from the jobs_list
          Creates queues, shared data, and the key processes of:
          Coordinator, Logger, and the Workers
          Factory waits until Coordinator quits, followed by Workers,
          followed by Logger.
          
        Returns:
          Boolean indicating successful completion.
        """
        # generated the jobs list
        # the IDs are indices 1-len(JobBounds) such that each job i
        # is really the job [bounds[i-1], bounds[i])
        jobs_list = range(1, len(self.Request.JobBounds))

        # write job mapping file
        writer = codecs.open(QueryFactory.jobmap_filename(self.FileStem),
                             'w', encoding='utf-8')
        for i in jobs_list:
            writer.write(self.AgentClass.data_filename(self.FileStem, i, int(ceil(log10(self.Request.jobCount)))))
            writer.write(u'\t')
            writer.write(QueryFactory.filterrange_text(self.Request.JobBounds[i-1],
                                                       self.Request.JobBounds[i]))
            writer.write(u'\n')
        writer.close()
            
        # if resume, check existence of existing files
        numWorkers = self.NumWorkers
        if self.ResumeWork:
            print("Checking previously completed work before resuming...")
            _unfinished = []
            for job in jobs_list:
                _filename = self.AgentClass.data_filename(self.FileStem, job, int(ceil(log10(self.Request.jobCount))))
                if not os.path.isfile(_filename):
                    _unfinished.append(job)

            if len(_unfinished) == 0:
                print("All jobs previously completed.")
                return True
            elif len(_unfinished) == len(jobs_list):
                print("No previous work found.")
            else:
                print(u"Workload reduced from " + unicode(len(jobs_list))
                      + u" job(s) to " + unicode(len(_unfinished)) 
                      + u" job(s).")
                if len(_unfinished) < numWorkers:
                    print(u"Temporarily reducing number of workers from "
                          + unicode(numWorkers) + u" to "
                          + unicode(len(_unfinished)) )
                    numWorkers = len(_unfinished)
            
            jobs_list = _unfinished
        #end if ResumeWork
        shuffle(jobs_list)

        print("Starting up Factory.")
        sleep(1.5) # just for show

        # create queues
        # job_queue is a joinable queue because it has multiple consumers
        # by waiting for the queue to join, we will know that the workers
        # have finished as well
        job_queue = multiprocessing.JoinableQueue()
        job_queue.cancel_join_thread()
        coordinator_queue = multiprocessing.Queue()
        coordinator_queue.cancel_join_thread()    
        log_queue = multiprocessing.Queue()
        log_queue.cancel_join_thread()
        # the above cancel_join_thread() calls make the processes not wait
        # for any data they enqueued to finish
    
        # create shared dictionary for tracking incomplete jobs
        shared_data = PersistentSyncManager()
        shared_data.start()
        last_jobs = shared_data.dict()

        # create shared counter for counting all returned rows
        record_count = shared_data.Value('i', 0)

        # create workers
        workers = []
        for i in xrange(numWorkers):
            path = self.Request.Paths[ i % len(self.Request.Paths) ]
            key = self.Request.Keys[ i % len(self.Request.Keys) ]
            w = Worker(self.Request, job_queue, coordinator_queue, log_queue,
                       last_jobs, self.FileStem, self.AgentClass,
                       path, key, self.AllowZeros)
            workers.append(w)
            w.name = "Worker-"+unicode(i+1)
            last_jobs[w.name] = None # prepare last jobs

        # create logger
        logfile = QueryFactory.log_filename(self.FileStem)
        log_outputs = [ codecs.open(logfile, 'w', encoding='utf-8') ]
        if self.ToStandardOut:
            log_outputs.append( sys.stdout )
        logger = Logger(log_queue=log_queue, output=log_outputs)
        logger.name = "Logger"

        # create coordinator
        coordinator = Coordinator(coordinator_queue=coordinator_queue,
                                  job_queue=job_queue, log_queue=log_queue, \
                                  jobs_list=jobs_list, num_workers=numWorkers, \
                                  record_count=record_count)
        coordinator.name = "Coordinator"
        
        # enqueue jobs
        for j in jobs_list:
            job_queue.put(j)

        # start up the factory
        try:
            # start processes in correct order:
            #   logger to wait for messages
            #   coordinator to oversee workers
            #   workers to do the work            
            logger.start()
            coordinator.start()
            for w in workers:
                w.start()

            # wait for processes to end in the correct order:
            #   coordinator to know all jobs are done and has issues quit orders
            #   job_queue ends when all workers have indicated their end
            #   logger gets the kill message when Coordinator and Worker are done
            coordinator.join()
            job_queue.join()
            log_queue.put(None) # send kill to the logger
            logger.join()

            print(u"Work completed. " + unicode(record_count.value)
                  + u" records collected.")
            
            return True
        except Exception as e:
            # Generally, this should only happen upon KeyboardInterrupts
            print("", file=sys.stderr)
            print("Process interrupted. Shutting down factory...",
                  file=sys.stderr)

            # terminate the workers first, but clean up any files that
            # are in progress. last_jobs only contains job IDs if that
            # worker has not finished the query
            for w in workers:
                print("... terminating " + w.name, file=sys.stderr)
                if last_jobs[w.name] is not None:
                    if os.path.isfile(last_jobs[w.name]):
                        os.remove(last_jobs[w.name])                    
                w.terminate()
                w.join()

            # terminate the coordinator
            print("... terminating Coordinator", file=sys.stderr)
            coordinator.terminate()
            coordinator.join()

            # terminate the logger
            print("... terminating Logger", file=sys.stderr)
            logger.terminate()
            logger.join()

            # kill the shared_data manager for good measure
            shared_data.shutdown()
        
            print("Factory Closed.", file=sys.stderr)

            return False
        #end Exception        
    #end engage

    @staticmethod
    def log_filename(stem):
        """
        Helper static function for getting the filename of a
        log file this Factory will produce based on the
        provided filestem.

        Parameters:
          stem   Filestem

        Returns:
          stem.log
        """
        return stem + u".log"

    @staticmethod
    def jobmap_filename(stem):
        """
        Helper static function for getting the filename of a
        job-mapping file this Factory will produce  based on
        the provided filestem.

        Parameters:
          stem   Filestem

        Returns:
          stem.jobs
        """
        return stem + u".jobs"

    @staticmethod
    def filterrange_text(filter1, filter2):
        """
        Helper static function for making a nicely formatted
        description of a filter range.

        Parameters:
          filterStart   Start of the filter range (inclusive)
          filterStop    End of the filter range (exclusive)

        Returns:
          [filterStart,filterStop)
        """        
        if filter1 is None:
            txt = u"(..."
        else:
            txt = u"[" + unicode(filter1)

        txt = txt + u","

        if filter2 is None:
            txt = txt + u"..."
        else:
            txt = txt + unicode(filter2)

        txt = txt + u")"
        return txt
    #end filterrange_text
        
#end class QueryFactory
    
class Worker(multiprocessing.Process):
    """
    The Worker is a Process subclass that handles the task of performing
    a specific query to an analytic. The worker identifies what query to
    make based on jobs pushed onto a job queue. The Worker also reports
    its progress to both a Logger and a Coordinator.
    """
    def __init__(self, request, job_queue, coordinator_queue, log_queue,
                 last_job, filestem, agent_class, path, apikey, allow_zeros):
        """
        Constructor for a Worker process. 

        Parameters:
        request             RequestObject for the queries
        job_queue           A JoinableQueue from which the worker receives
                            job IDs that determine the next query
        coordinator_queue   A Queue to which the worker posts success or
                            failure of each job as tuple of:
                            (jobID, # results, boolean)
        log_queue           A Queue to which the worker posts strings
                            describing its progress.
        last_job            A shared dictionary to which the worker places
                            the ID of the current job it is working on at
                            last_job[worker.name].
                            This entry is set to None each time a job
                            completes. The job IDs in last_job are used to
                            clean up incomplete files upon unexpected
                            closing of the factory.
        filestem            The filestem used for any output files.
        agent_class         The class object (a subclass of AnalyticAgent)
                            for the self.agent object the worker creates
                            to perform the query.
        path                The path to the Analytic the worker will use 
                            for all queries
        apikey              The apikey the worker will always use for all
                            queries to the Analytic.
        allow_zeros         Boolean for indicating if zero results is a
                            valid result.
        """
        
        multiprocessing.Process.__init__(self)
        self.daemon = True
        
        self.request = request
        self.job_queue = job_queue
        self.coordinator_queue = coordinator_queue
        self.log_queue = log_queue
        self.filestem = filestem
        self.agent = agent_class.loadRequest(self.request)
        self.path = path
        self.apikey = apikey
        self.allow_zeros = allow_zeros        
        self.last_job = last_job
    #end init()
        
    def run(self):
        """
        Override of the process's run method. When called, the worker
        will continue to pull jobs from job_queue until it receives a
        None job. Note that upon any completion (success or fail), the
        Worker must signal job_queue.task_done() as job_queue is a
        JoinableQueue.

        Process:
          Grab a JobID j
          Interpret j as filtering JobBounds[j-1] to JobBounds[j]
          Create output writer
          Set self.last_job[self.name] to j
          Run agent
          If successful completion (no exceptions):
            Close output writer
            Set self.last_job[self.name] to None
            Inform Coordinator
          Else:
            Clean up incomplete output file
            Inform coordinator
            Sleep a little before trying again 
        """
        self.log_queue.put(strftime("%H:%M:%S") + u" : " + unicode(self.name) + u"> started")
        self.log_queue.put(strftime("%H:%M:%S") + u" : " + unicode(self.name) + u"> Path: " + self.path)
        self.log_queue.put(strftime("%H:%M:%S") + u" : " + unicode(self.name) + u"> APIkey: " + self.apikey)
        startIndex = ""
        try:
            while True:
                jobID = self.job_queue.get()
                if jobID is None:
                    # Poison pill indicates to shutdown this worker
                    self.log_queue.put( strftime("%H:%M:%S") + " : " + unicode(self.name)
                                        + u"> shutting down")
                    self.job_queue.task_done()
                    break

                filterStart = self.request.JobBounds[jobID-1]
                filterStop = self.request.JobBounds[jobID]

                self.log_queue.put(strftime("%H:%M:%S") + u" : "  + unicode(self.name) 
                                   + u"> starting job " + unicode(jobID) + u" "
                                   + QueryFactory.filterrange_text(filterStart,filterStop))

                filename = self.agent.data_filename(self.filestem, jobID, int(ceil(log10(self.request.jobCount))))
                outfile = codecs.open(filename,'w', encoding='utf-8')
                self.last_job[self.name] = filename                
                try:
                    n = self.agent.run(unicode(self.name), writer=outfile, logger=[self.log_queue],
                                       allowZeros=self.allow_zeros, path=self.path, apikey=self.apikey,
                                       limit=1000,filterStart=filterStart, filterStop=filterStop,
                                       queryType=QueryType.ALL)
                    self.log_queue.put(strftime("%H:%M:%S") + u" : " + self.name + u"> completed job "
                                       + unicode(jobID) + u" "
                                       + QueryFactory.filterrange_text(filterStart,filterStop)
                                       + u" and found " + unicode(n) + u" records" )
                    outfile.close()
                    self.last_job[self.name] = None
                    
                    # message success to manager (index, true for success)
                    self.coordinator_queue.put( (jobID, n, True) )
                    self.job_queue.task_done()

                except AnalyticServerError as e:
                    self.log_queue.put(strftime("%H:%M:%S") + u" : " + self.name
                                       + u"> failed due to server error:\n"
                                       + unicode(e) )
                    outfile.close()
                    # clean up the incomplete outfile
                    if os.path.isfile(filename):
                        os.remove(filename)
                        self.last_job[self.name] = None                        
                    # message failure to manager (index, false of failure)                    
                    self.coordinator_queue.put( (jobID, None, False) )
                    self.job_queue.task_done()
                    # sleep a little to avoid getting the same job again                    
                    sleep(2.5 + uniform(-2,-2))

                except ZeroResultsError as e:
                    self.log_queue.put(strftime("%H:%M:%S") + u" : " + self.name
                                       + u"> failed due to zero results error" )
                    outfile.close()                    
                    # clean up the incomplete outfile
                    if os.path.isfile(filename):
                        os.remove(filename)
                        self.last_job[self.name] = None                        
                    # message failure to manager (index, false of failure)                    
                    self.coordinator_queue.put( (jobID, None, False) )
                    self.job_queue.task_done()
                    # sleep a little to avoid getting the same job again
                    sleep(2.5 + uniform(-2,-2))

            # end while True loop
        except KeyboardInterrupt as e:
            print("Interrupt in: " + unicode(self.name))
            raise e
        return 
    # end run function
# end worker class

class Logger(multiprocessing.Process):
    """
    The Logger class is a standalone process for outputting status
    messages received on a dedicated logging queue. These messages
    come from the Workers and the Coordinator.
    """
    def __init__(self, log_queue, output):
        """
        Constructor for the Logger class.
        
        Parameters:
        log_queue    A Queue object from which the Logger will grab
                     messages until it receives a None.
        output       A file object (or iterable container of many such
                     objects) to  which the Logger will output the
                     messages to.

        Throws:
          TypeError if output or element in output does not have a
          write(...) method
        """
        multiprocessing.Process.__init__(self)
        self.daemon = True
        
        self.log_queue = log_queue
        if isinstance(output, basestring):
            self.output = [output]
        else:
            try: iter(output)
            except TypeError: self.output = [output]
            else: self.output = list(output)

        for out in self.output:
            if not hasattr(out, "write"):
                raise TypeError(u"Logger __init__(): parameter output '" + unicode(out)
                                + "u' does not have a write(...) method" )
        
    def run(self):
        """Override of the Process run() method for Logger.

        Logger will continually  pull messages from its log_queue until
        receiving a None message to quit. Each message will be outputted
        to the channel(s) contained in self.outputs.

        Throws:
          KeyboardInterrupt:
          If this process encounters a KeyboardInterrupt, it tosses
          it up again to be ultimately caught by the QueryFactory.

        """
        try:
            while True:
                line = self.log_queue.get()
                if line is None:
                    break
                else:
                    for out in self.output:
                        out.write(unicode(line.strip()) +u"\n")
            # end while True loop
            return
        except KeyboardInterrupt as e:
            print("Interrupt in: " + unicode(self.name))
            raise e        
        except:
            pass
    # end run
# end Logger 

class Coordinator(multiprocessing.Process):
    """
    The Coordinator is a standalone process that keeps track of what
    jobs have been completed by the workers. Importantly, if a job fails,
    it will re-enqueue the job onto the job_queue for another worker to
    attempt (unless the job has failed more than a specified tolerance).
    Once all jobs have been completed or abandoned, the coordinator sends
    a quit message to all workers.
    """
    
    def __init__(self, coordinator_queue=None, job_queue=None, log_queue=None,
                 jobs_list=None, num_workers=None, record_count=None,
                 attempted_tolerance=5):
        """Constructor for the Coordinator:
        Parameters:
        coordinator_queue  A queue from which the coordinator pulls status
                           reports (a tuple of jobID, # results, boolean
                           success)posted by the Workers.
        job_queue          A queue to which the Coordinator reposts failed
                           jobs or quit (None) messages to the Workers.
        log_queue          A queue to which the Coordinator posts status
                           messages for logging purposes.
        jobs_list          A list of the jobs (RequestObject.JobBounds)
                           indices) that need to be completed.
        num_workers        The integral number of workers in the factory.
        record_count       A shared memory integer for keeping track of
                           how many total records have been found by the
                           workers.
        attempt_tolerance  An integer value indicating how many times a
                           job should be attempted before it should be
                           abandoned.
        """
        multiprocessing.Process.__init__(self)
        self.daemon = True
        
        self.coordinator_queue = coordinator_queue
        self.jobs_list = jobs_list[:] # we want a copy

        self.job_attempts = {}
        for i in xrange(1, len(jobs_list)):
            self.job_attemps[ jobs_list[i] ] = 0
            
        self.job_queue = job_queue
        self.log_queue = log_queue
        self.num_workers = num_workers
        self.AttemptTolerance = attempted_tolerance
        self.abandoned_jobs = []
        self.record_count = record_count
        
    def run(self):
        """
        Override of the Process's run() method.

        The Coordinator will continually pull messages from its
        coordinator_queue until it receives a None or self.jobs_list
        becomes empty. Each message is a tuple of the form:
        (JobID, results, success boolean) where JobID is an index
        into a RequestObject.JobBounds list.

        If the job is a success:
          The Coordinator removes job from jobs_list and records
          the number of results found
        If the job is a failure:
          The Coordinator updates its internal count of how many
          times jobID has previously failed. If that number exceeds
          the attempt_tolerance, that job is placed into the list of
          abandoned_jobs. Otherwise, the jobID is placed back onto
          job_queue.

        If at this point, jobs_list is empty:
          The coordinator pushes num_workers amount of quit messages
          (None) onto the job_queue and shuts itself down.

        Throws:
          KeyboardInterrupt:
          If this process encounters a KeyboardInterrupt, it tosses
          it up again to be ultimately caught by the QueryFactory.
        """
            
        try:
            while True:
                msg = self.coordinator_queue.get()
                if msg is None:
                    self.log_queue.put( strftime("%H:%M:%S") + u" : Coordinator> shutting down")
                    break
                last_job = msg[0]
                count = msg[1]
                success = msg[2]
                if success:
                    # remove job from jobs_list
                    self.jobs_list.remove(last_job)
                    self.log_queue.put( strftime("%H:%M:%S") + u" : Coordinator> Job "
                                        + unicode(last_job) + u" completed")
                    self.record_count.value += count
                    self.log_queue.put( strftime("%H:%M:%S") + u" : Coordinator> "
                                        + "Total records found so far: "
                                        + unicode(self.record_count.value) )
                else: # job failed
                    # record failed attempt
                    self.job_attempts[last_job] += 1

                    # check to see if we have exceeded our attempt tolerance
                    if self.job_attempts[last_job] >= self.AttemptTolerance:
                        # this job is doomed to failure
                        self.abandoned_jobs.append(last_job)
                        self.jobs_list.remove(last_job)
                        self.log_queue.put( strftime("%H:%M:%S") + u" : Coordinator> Job "
                                            + unicode(last_job) + u" has failed "
                                            + unicode(self.job_attempts[last_job])
                                            + u" time(s). Abandoning job.")                        
                    else:
                        # re-add job to job_queue
                        self.job_queue.put(last_job)
                        self.log_queue.put( strftime("%H:%M:%S") \
                                            + u" : Coordinator> Failed job " \
                                            + unicode(last_job) + u" re-enqueued" )

                if len(self.jobs_list) > 0:
                    self.log_queue.put( strftime("%H:%M:%S") + u" : Coordinator> " \
                                        + unicode(len(self.jobs_list)) + u" job(s) remaining ("
                                        + unicode(len(self.abandoned_jobs)) + " abandoned)")
                else: # ALL JOBS DONE!!!
                    self.log_queue.put( strftime("%H:%M:%S") + u" : Coordinator> " \
                                        + u"All jobs completed ("
                                        + unicode(len(self.abandoned_jobs)) + " abandoned)")
                    sleep(0.05)
                    self.log_queue.put( strftime("%H:%M:%S") + u" : Coordinator> "
                                        +"Firing workers and shutting down..." )

                    # push None jobs to fire every worker
                    for i in xrange(self.num_workers):
                        self.job_queue.put(None)                   
                    break                    
            # end while True loop
            return
        except KeyboardInterrupt as e:
            print("Interrupt in: " + unicode(self.name))
            raise e        
        except Exception as e:
            print("Coordinator exception: " + unicode(e))
            pass
    # end run
# end Coordinator

class PersistentSyncManager(multiprocessing.managers.SyncManager):
    """
    This SyncManager performs exactly like a regular SyncManager except 
    that upon starting, it will ignore any KeyboardInterrupts sent to it.
    Thus, the process for this manager will not be terminated and thus
    any shared data contained within it will still be available.
    """

    # Idea/code found at:
    # http://stackoverflow.com/questions/21104997/keyboard-interrupt-with-pythons-multiprocessing
    def __init__(self):
        """
        Basic constructor. Does nothing more than call the base
        constructor.
        """
        multiprocessing.managers.SyncManager.__init__(self)

    def start(self):
        """
        Override of the start() method that includes code to force
        the manager to ignore any keyboard interrupts / SIGINTs
        """
        signal.signal(signal.SIGINT, self.sigint_handler)
        multiprocessing.managers.SyncManager.start(self)

    def sigint_handler(self, signal, frame):
        """
        This dummy method (it does nothing) is what ignores the
        interrupts. For debugging purposes, a print statement can
        be placed inside to indicate whenever a keyboard interrupt
        is intercepted and ignored.
        """
        # print("Ignoring SIGINT interrupt")
        pass


