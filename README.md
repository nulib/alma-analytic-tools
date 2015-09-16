# University of Washington Libraries Alma Analytic Tools #
Alma's analytics are powerful tools for accessing, collecting, and exploring the various information contained within an Alma repository. Despite options such as reports and dashboards for communicating any analytic results, sometimes one needs access to the raw data itself. For such purposes, one must use the Alma RESTful APIs and perform one or more HTTP GET requests. This package provides a Python implementation to simplify and support that process.

## Content Highlights ##

`page_download.py`  

: Executable python script that performs a single, interactive query to an analytic

`report_download.py`  

: Executable python script that uses a *RequestObject* file that performs multiple queries to download up to 65001 rows of data.

`download_all.py`  

: Executable python script that can use parallelism via the *QueryFactory* to download ALL data (even more than 65001 rows) from an analytic.

`input_examples/`  

: Folder containing examples of input files for use with the above scripts

## Requirements ##

The code found in this repository was written in Python 2 (developed and tested in 2.6) in a Linux/Unix environment. In addition to standard Python packages, two additional packages are required.

* [bidict package](https://pypi.python.org/pypi/bidict/0.3.1): bidirectional dictionary object
* [enum34 package](https://pypi.python.org/pypi/enum34/1.0): backport of Python 3.4.'s enum package

The versions used in the development of this code are included in this repository for convenience. We hold no claim to these files.

One additional package is recommended for performance reasons but is not absolutely required:

* [lxml](http://lxml.de/): CPython library for faster XML processing

## Documentation ##
Each file contains detailed python docstrings and comments. In addition, this wiki also contains the following documents:

[Understanding Analytic GET Requests](https://bitbucket.org/uwlib/uwlib-alma-analytic-tools/wiki/Understanding_Analytic_GET_Requests)
: This page details various issues and challenges in using the API to query analytic data. Error states and limitations are discussed. Most importantly, this page describes how to download all data from an analytic even if it breaks the hardset 65001 limit.

[Request Object File Format](https://bitbucket.org/uwlib/uwlib-alma-analytic-tools/wiki/Request_Object_File_Format)
: An in-depth discussion of how to write the input files for creating a ##RequestObject##.

[Current Issues and Future Work](https://bitbucket.org/uwlib/uwlib-alma-analytic-tools/wiki/Current_Issues_and_Future_Work)
: A description of the current status of the code and its potential next directions.

[HathiTrust Print Holdings Download](https://bitbucket.org/uwlib/uwlib-alma-analytic-tools/wiki/HathiTrust%20Print%20Holdings)
: Details on how to use the code for downloading [print holdings for HathiTrust](http://www.hathitrust.org/print_holdings).

; [[https://bitbucket.org/uwlib/uwlib-alma-analytic-tools/raw/master/images/deibel-alma-analytics-api.pptx|Alma Analytics API Talk]]
: PowerPoint slides from a talk I gave at the 2015 Summer Meeting for the Orbis Cascade Alliance on the quirks of using the Alma Analytic API.

## License ##
This code is released under the OpenBSD license: 

Copyright (c) 2014 Katherine Deibel

Permission to use, copy, modify, and distribute this software for any
purpose with or without fee is hereby granted, provided that the above
copyright notice and this permission notice appear in all copies.

THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.