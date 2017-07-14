# Feature Extraction

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

extract_features.py
-------------------

Before running this program, ensure that you have installed the `scipy` library
for Python: https://www.scipy.org/install.html

There are a number of different ways that you can run `extract_features.py`.  <b>If
you would like to extract features from an AMT results file (such as that created
by `merge_amt_results.py`) and you also have expert labels for those 
instances</b>, you should uncomment lines 737, 741, and 742, and comment lines 
738, 739, 740, and 743.  These lines should already be commented/uncommented
if you have not modified the original version of the file in any way.

Running the program in this way will produce a total of 7 output files:
* `<prefix>_label_dataset.csv`: Extracted features for every instance.  The first column will contain a unique ID for each instance.
* `<prefix>_label_dataset_train.csv`: A random subset of instances assigned to the training set.
* `<prefix>_label_dataset_validation.csv`: A random subset of instances assigned to the validation set.
* `<prefix>_label_dataset_test.csv`: A random subset of instances assigned to the test set.
* `<prefix>_train_mace_vector.csv`: An <i>n</i>-dimensional vector for each instance in the training set, with one column corresponding to each unique annotator in the dataset.  For a given row (instance), filled columns are associated with annotators who annotated that instance.  This format is used as input to MACE, used in the evaluation for the Item-Response approach.
* `<prefix>_validation_mace_vector.csv`: A MACE-formatted input vector for the validation set.
* `<prefix>_test_mace_vector.csv`: A MACE-formatted input vector for the test set.

You can specify `<prefix>` by setting the corresponding parameter in `self.create_data_files()`
(line 742).

Before running `extract_features.py`, you will need to set four variables in its
Main() function:
* `self.input_dir`: Line 732: The directory in which your input files are located.
* `self.output_dir`: Line 733: The directory in which your output files are located.
* `worker_filename`: Line 734: The AMT results file.
* `expert_filename`: Line 735: The file containing expert annotations for the same instances.

Once you've set those variables, you should be able to run the program as follows:
```
python extract_features.py
```

<b>If you would like to extract features from an AMT results file but you do not
have expert labels for those instances</b>, you should comment lines 737, 739,
740, and 742, and uncomment lines 738 and 743.  Set the same variables indicated
previously (you do not need to set `expert_filename`).  Running the program this way
will produce a single output file, `unlabeled_crowdsourced_instances.csv`, which
contains features for each instance but the final label column is left empty (with a
value of "?").  Run the program as follows:
```
python extract_features.py
```

Finally, if you would like to extract features from the third-party datasets
referenced in the paper cited above, you may do so as follows:

<b>Affect (Emotion)</b> or <b>Affect (Valence)</b>:

Comment lines 737, 738, 740, and 743.  Uncomment lines 739 and 742.  Set
`self.input_dir`, `self.output_dir`, and `worker_filename` as usual
(`worker_filename`, the file containing the dataset, will contain both
crowdsourced and expert labels).  Run the program as follows:
```
python extract_features.py
```

<b>WebRel</b>:

Comment lines 737, 738, 739, and 743.  Uncomment lines 740 and 742.  Set
`self.input_dir`, `self.output_dir`, and `worker_filename` as usual
(`worker_filename`, the file containing the dataset, will contain both
crowdsourced and expert labels).  Run the program as follows:
```
python extract_features.py
```


Contact
=======

If you have any questions about this code, please contact me through GitHub or at:
natalie.parde@unt.edu.

If you use this code or the sample data in any way, please cite the paper
referenced earlier.

Thanks!
