# Evaluation

This subdirectory contains the code necessary to train the label aggregator, run
it on new test instances, and evaluate its performance.  Much more information
can be found in:

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

train_and_test.py
-----------------

Before running this program, you will need to install the following dependencies
if they are not on your system already:
* `scipy`: https://www.scipy.org/install.html
* `scikit-learn`: http://scikit-learn.org/stable/install.html
* `Weka`: http://www.cs.waikato.ac.nz/ml/weka/downloading.html  (Note: This program was originally run using Weka 3.8.)
* `python-weka-wrapper`: http://pythonhosted.org/python-weka-wrapper/install.html

You can run `train_and_test.py` with a variety of feature subsets (these
feature subsets correspond to the subsets described in the paper cited earlier):
* <b>All:</b> (Default) Uncomment line 135, and comment lines 136-140.
* <b>All - Annotations:</b> Uncomment line 136, and comment lines 135 and 137-140.
* <b>All - Avg. R:</b> Uncomment line 137, and comment lines 135, 136, and 138-140.
* <b>All - Avg. R (Good):</b> Uncomment line 138, and comment lines 135-137, 139, and 140.
* <b>All - Averages:</b> Uncomment line 139, and comment lines 135-138 and 140.
* <b>All - HIT R:</b> Uncomment line 140, and comment lines 135-139.

The program will generate three output files:
* `continuous_aggregation_predictions_<feature_set>.csv`: The system's continuous prediction and the corresponding gold standard value for each instance.
* `discrete_aggregation_predictions_<feature_set>.csv`: The system's discrete prediction and the corresponding gold standard value for each instance.
* `results_<feature_set>.csv`: A results file containing the correlation coefficient and RMSE for each prediction type, as well as accuracy for the discrete predictions.

Before running `train_and_test.py`, you will need to set eight variables in its
Main() function:
* `self.input_dir`: Line 127: The directory in which your input files are located.
* `self.output_dir`: Line 128: The directory in which your output files are located.
* `train_dataset`: Line 130: The name of your training file.  This and the file indicated in `test_dataset` should both be comma-separated files, with one instance per line and one feature per column.  Refer to `sample_input/training_and_validation.csv` if using your own training file to make sure it is formatted correctly.
* `test_dataset`: Line 131: The name of your test file.
* The `packages` parameter in jvm.start(): Line 132: The path to your wekafiles directory.  This directory should automatically be installed when you install Weka on your machine.
* `train_name`: Line 143: The data you're using for training.  This will be written for reference in `results_<feature_set>.csv`.
* `test_name`: Line 144: The data you're using to test the trained model (will also be written for reference in `results_<feature_set>.csv`).
* `feature_set`: Line 145: The name of the feature subset you are using.  This will be used as a suffix for your output files.

Once you have set those variables, you should be able to run the program as follows:
```
python train_and_test.py
```

Contact
=======

If you have any questions about this code, please contact me through GitHub or at:
natalie.parde@unt.edu.

If you use this code or the sample data in any way, please cite the paper
referenced earlier.

Thanks!
