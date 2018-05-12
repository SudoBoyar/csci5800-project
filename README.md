# Big Data Systems Project

### tar_processing:
The purpose of this step is to work with the original raw tar files, and get the data stripped down to only English tweets, and to reduce the data being stored for each tweet down to only the required fields for each tweet.

The directory contains a bash script to extract the tar archives and then run the python script.

The python script combines the per minute files into a single file per hour. It then groups the month into 3 day groups, which is the largest length of time that was consistently able to be run as a single job without errors on the cluster being used at the time. The script then produces a monthX.pig script for each group (X being the group number), as well as a .sh file that will run every group one after the other.


### semantic_analysis:
The purpose of this step is to perform the semantic analysis on the cleaned dataset.

Note: Due to an error in the handling of the raw data stripping, the utility_scripts/remove_newlines.sh must be run on every file produced by the first step. This error is easily fixed, but has been left to preserve the process we used.



### utility_scripts:
A collection of scripts that were used during various parts of the process to perform miscelaneous tasks.


### custom_data:

Data we produced.

* regions.csv: A list of regions for the US.
* timezone_region.csv: A mapping of timezones to their corresponding geographic regions.

Data files needed:

Full Data Source:
https://archive.org/details/twitterstream?and%5B%5D=year%3A%222017%22&sort=-date
Sample Data:
https://drive.google.com/open?id=1UM3uvfqYD7d2ZrDYbZCbcXf_-5jW-uy_

Data for ComparativeAnalyzer.ipynb:
https://drive.google.com/open?id=16ZR01kVv3w8JTI8oJ-aPUs2L4JMNPgPx
