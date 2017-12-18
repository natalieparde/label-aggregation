###############################################################################
# train_and_test.py
#
# Natalie Parde
# 4/12/2017
#
# Trains and tests the label aggregation approach on the specified training
# and test sets, and outputs the results to a CSV file.
#
###############################################################################

import os
import csv
import time
from glob import glob
from itertools import chain, combinations
import scipy.stats
import weka.core.converters as converters
import weka.core.jvm as jvm
from math import sqrt
from weka.filters import Filter
from weka.classifiers import Classifier, Evaluation, Kernel
from weka.core.dataset import Instance, Attribute
from weka.core.classes import from_commandline
from sklearn.metrics import mean_squared_error, accuracy_score

class TrainAndTest:

   # Load the dataset.
   def read_data(self, train_file, test_file, remove_features=[0]):
      self.train_data = converters.load_any_file(self.input_dir + "/" + train_file)
      self.test_data = converters.load_any_file(self.input_dir + "/" + test_file)

      # Remove the first attribute from each dataset; it's just an identifier.
      remover = Filter(classname="weka.filters.unsupervised.attribute.Remove", options=["-R", ",".join(str(f+1) for f in remove_features)])  # Weka's "remover" starts at 1, not 0.  Thus, add one to each feature id (they were zero-based).
      remover.inputformat(self.train_data)
      self.filtered_train_data = remover.filter(self.train_data)
      self.filtered_train_data.class_is_last()

      remover.inputformat(self.test_data)
      self.filtered_test_data = remover.filter(self.test_data)
      self.filtered_test_data.class_is_last()

      # Get the names of the included features.
      # Comment out during ablation studies!
      infile = open(self.input_dir + "/" + train_file)
      line_counter = 0
      feature_ids = []
      for line in infile:
         if line_counter == 0:
            self.features = line.split(",")
            for x in range(1,len(self.features)-1):  # Ignore identifier and label here (we always want to remove the identifier and never want to remove the label).
               feature_ids.append(x)
         line_counter += 1
      infile.close()

      features_used = []
      for x in range(1,len(self.features)):
         if x not in remove_features:
            features_used.append(self.features[x])
      features_used_str = ", ".join(features_used).strip().strip(",")
      print "Using features:", features_used_str

   # Train the specified classifier on the training data, and test it on the 
   # test data.  Output the results to a CSV file.
   def experiment(self, train_set, test_set, label_type, feature_set):
      results = []
      predictions_file1 = open(self.output_dir + "/continuous_aggregation_predictions_" + feature_set + ".csv", "w")
      predictions_file2 = open(self.output_dir + "/discrete_aggregation_predictions_" + feature_set + ".csv", "w")
      predictions_writer1 = csv.writer(predictions_file1)
      predictions_writer2 = csv.writer(predictions_file2)
      classifier = Classifier(classname=self.classifier_name)
      classifier.build_classifier(self.filtered_train_data)

      # Create arrays containing the predicted and true values for each instance.
      predictions_rounded = []
      predictions_continuous = []
      predictions_writer1.writerow(["Index", "Prediction", "True Value"])
      predictions_writer2.writerow(["Index", "Prediction", "True Value"])
      true_values = []
      for index, instance in enumerate(self.filtered_test_data):
         prediction = classifier.classify_instance(instance)
         predictions_rounded.append(int(round(prediction)))
         predictions_writer2.writerow([index, int(round(prediction)), float(str(instance).split(",")[-1])])
         predictions_continuous.append(prediction)
         true_values.append(float(str(instance).split(",")[-1]))
         predictions_writer1.writerow([index, prediction, float(str(instance).split(",")[-1])])

      # Get the correlation coefficient between the arrays using sklearn.
      slope, intercept, r_round, p_value, std_err = scipy.stats.linregress(predictions_rounded, true_values)
      rms_round = sqrt(mean_squared_error(true_values, predictions_rounded))
     # accuracy = accuracy_score(true_values, predictions_rounded)
      result_line = [self.classifier_name.replace("weka.classifiers.", ""), train_set, test_set, label_type + " (Rounded)", r_round, rms_round, "N/A"]
      results.append(result_line)

      slope, intercept, r_continuous, p_value, std_err = scipy.stats.linregress(predictions_continuous, true_values)
      rms_continuous = sqrt(mean_squared_error(true_values, predictions_continuous))
      result_line = [self.classifier_name.replace("weka.classifiers.", ""), train_set, test_set, label_type + " (Continuous)", r_continuous, rms_continuous, "N/A"]
      results.append(result_line)
      predictions_file1.close()
      predictions_file2.close()
      return results

   # Get predictions from the classifier in a list format.
   def get_predictions(self):
      self.predictions_rounded = []
      self.predictions_continuous = []
      classifier = Classifier(classname=self.classifier_name)
      classifier.build_classifier(self.filtered_train_data)

      # Create arrays containing the predicted and true values for each instance.
      for index, instance in enumerate(self.filtered_test_data):
        prediction = classifier.classify_instance(instance)
        self.predictions_rounded.append(int(round(prediction)))
        self.predictions_continuous.append(prediction)

   # Output the results to a CSV file.
   def output_results(self, results, feature_set):
      outfile = open(self.output_dir + "/results_" + feature_set + ".csv", "w")
      writer = csv.writer(outfile, quotechar='"')
      writer.writerow(["Classifier", "Training Set", "Test Set", "Label Type", "Correlation Coefficient", "Root Mean Squared Error", "Accuracy"])
      for row in results:
         writer.writerow(row)
      outfile.close()

   def Main(self):
      self.input_dir = "sample_input"
      self.output_dir = "sample_output"
      self.classifier_name = "weka.classifiers.meta.RandomSubSpace"
      train_dataset = "gold_label_dataset_train+validation.csv"
      test_dataset = "gold_label_dataset_test.csv"
      jvm.start(packages="/home/parde/wekafiles")  # Path to the wekafiles directory (installed with Weka).
      
      # Uncomment the read_data version that uses your preferred feature set.
      self.read_data(train_dataset, test_dataset)  # All
      #self.read_data(train_dataset, test_dataset, remove_features=[0, 5, 8, 11, 14, 17, 20, 22, 24, 26, 28])  # All - Annotations.
      #self.read_data(train_dataset, test_dataset, remove_features=[0, 6, 9, 12, 15, 18, 21, 23, 25, 27, 29])  # All - Avg. R
      #self.read_data(train_dataset, test_dataset, remove_features=[0, 7, 10, 13, 16, 19])  # All - Avg. R (Good)
      #self.read_data(train_dataset, test_dataset, remove_features=[0, 1, 2, 3])  # All - Averages.
      #self.read_data(train_dataset, test_dataset, remove_features=[0, 4])  # All - HIT R
      self.get_predictions()

      train_name = "Train+Validation"
      test_name = "Test"
      feature_set = "all"
      results = self.experiment(train_name, test_name, "Numeric", feature_set)
      self.output_results(results, feature_set)

      jvm.stop()

if __name__ == "__main__":
    train_and_test = TrainAndTest()
    train_and_test.Main()
