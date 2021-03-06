# Filtering Algorithm

This subdirectory contains code for automatically filtering HITs as they are
received.  It does so on the basis of presumed worker quality and completion
time; many more details can be found in the following paper:

Natalie Parde and Rodney D. Nielsen. Finding Patterns in Noisy Crowds: 
Regression-based Annotation Aggregation for Crowdsourced Data. To appear in the 
<i>Proceedings of the Conference on Empirical Methods in Natural Language 
Processing (EMNLP 2017)</i>. Copenhagen, Denmark, September 7-11, 2017.

Sample input and output files are provided in the directories `sample_input` and
`sample_output`, respectively.  Note that all Amazon Mechanical Turk worker IDs
have been anonymized, and the anonymized IDs do not necessarily match those of
the data released elsewhere in this repository.

The files output by the filtering algorithm will include:
* `disqualified_list.txt`: A list of the worker IDs that should be disqualified from completing future HITs.
* `double-pay_list.txt`: A list of particularly good workers to whom you may want to consider awarding a bonus.
* `hitwise_r_info.tsv`: Correlation statistics for each worker, for each HIT that worker completed.
* `r_value_info.tsv`: Correlation statistics for each HIT.  For the weighted correlation values, weights correspond to each individual worker's average r-value.
* `processed_<original_filename>`: A copy of the original file being processed, edited to accept and reject workers based on the algorithm's results.
* `<original_combined_filename>_qa_stage[0+].tsv`: The filtering algorithm iterates some number of times until it reaches a predetermined stopping point or the lists of workers converge.  The system outputs a snapshot of the statistics associated with each worker following each iteration of the algorithm.

Instructions for Running
========================

filter_hits.py
--------------

Before running this program, ensure that you have installed the `scipy` library
for Python: https://www.scipy.org/install.html

To run the code, you will need to set four variables in the program's Main() function:
* `self.input_dir`: Line 891: Set this to the directory containing your input files.
* `self.output_dir`: Line 892: Set this to the directory to which you would like the program to write its output.
* `self.filename`: Line 893: Set this to a combined file that contains both HITs that you have already accepted/rejected, and newly-submitted HITs.  If you do not have any accepted/rejected HITs yet, just set this to the same as the file below.  Note: Make sure that your columns are correctly aligned in your combined file!
* `new_hits_filename`: Line 894: Set this to the file containing submitted HIT assignments that you have just downloaded from Amazon Mechanical Turk.

After you have set these variables, simply run the code as follows:
```
python filter_hits.py
```

merge_amt_batches.py
--------------------
To easily generate the combined file that contains both existing HITs and HITs
from a new batch, you can use the provided script, `merge_amt_batches.py`.  To
run the script, you'll need to set four variables in the program's Main() function:
* `input_dir`: Line 148: Set this to the directory containing your input files.
* `output_dir`: Line 149: Set this to the directory containing your output files.
* `existing_hit_file`: Line 150: Set this to a file containing your existing HITs.
* `new_hit_file`: Line 151: Set this to a file containing your new HITs.  Note that there should be no overlap in HITs between `existing_hit_file` and `new_hit_file` (e.g., as in the case of getting additional assignments after rejecting some HITs)!  Otherwise you'll end up with duplicates in your combined HIT file.

After you have set these variables, simply run the code as follows:
```
python merge_amt_batches.py
```

The program will output a time-stamped, combined HIT file to `sample_output`.
If you leave the variables as they are, the program will combine 
`sample_input/anonymized_combined_hits_1-90.csv` and
`sample_input/anonymized_new_hits_91-100.csv` to create a merged file
identical to `sample_input/anonymized_combined_hits_1-100.csv`.


anonymize_worker_ids.py
-----------------------
Note that an additional script, `anonymize_worker_ids.py`, is also included in
`sample_input`.  This script was used to anonymize the worker IDs in the sample
data before releasing it publicly.  You shouldn't need to use this script, but
if you would like to for any reason, you can just run it as follows:
```
python anonymize_worker_ids.py
```

It'll create copies of whatever input files you specify at line 75, prefixed
with "anonymized_".  The copies will have anonymized worker IDs.

Contact
=======

If you have any questions about this code, please contact me through GitHub or at:
natalie.parde@unt.edu.

If you use this code or the sample data in any way, please cite the paper
referenced earlier.

Thanks!
