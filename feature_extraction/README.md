# Feature Extraction

(In Progress)

This subdirectory contains the code necessary to extract features from the
crowdsourced annotations contained in an Amazon Mechanical Turk results file.
Much more information about this process can be found in:

Natalie Parde and Rodney D. Nielsen. Finding Patterns in Noisy Crowds: 
Regression-based Annotation Aggregation for Crowdsourced Data. To appear in the 
<i>Proceedings of the Conference on Empirical Methods in Natural Language 
Processing (EMNLP 2017)</i>. Copenhagen, Denmark, September 7-11, 2017.

Sample input and output files are provided in the directories `sample_input` and
`sample_output`, respectively.  Note that all Amazon Mechanical Turk worker IDs
have been anonymized, and the anonymized IDs do not necessarily match those of
the data released elsewhere in this repository.

Instructions for Running
========================

merge_amt_results_files.py
--------------------------

To extract the features, you'll need to have all of your crowdsourced annotations
in a single file.  You may have already combined your annotations using
``../filtering/merge_amt_batches.py``; if so, you shouldn't need to use this script.

If you do have a bunch of individual AMT files though, you can combine them all
at once using the provided script, ``merge_amt_results_files.py``.  You'll need
to set three variables in the program's Main() function:
* `input_dir`: Line 167: Set this to the directory containing your input files.
* `output_dir`: Line 168: Set this to the directory containing your output files.
* `pattern`: Line 169: The pattern for the program to check for in `input_dir`.  This script will merge <i>all</i> files whose names adhere to that pattern, so be careful when setting it!

Once you've set those three variables, you should be able to run the program as follows:
```
python merge_amt_batches.py
```

The program will write a single merged file containing all of the instances in 
all of the files matching the specified pattern, with columns properly aligned,
to `sample_output`.  The output filename will be formatted as `<pattern>_annotations.csv`.


Contact
=======

If you have any questions about this code, please contact me through GitHub or at:
natalie.parde@unt.edu.

If you use this code or the sample data in any way, please cite the paper
referenced earlier.

Thanks!
