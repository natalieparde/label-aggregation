###############################################################################
# extract_features.py
#
# Natalie Parde
# 4/12/2017
#
# Extracts features from crowdsourced annotations, separates instances into
# training, validation, and test subsets, and outputs the data to several
# CSV files.
#
###############################################################################

import csv
import random
import operator
import scipy.stats
from collections import defaultdict

class AdjudicationInstanceBuilder:

   # Read the data into several dictionaries:
   # annotationXhit_workers: key: annotation ID -> value: HIT ID (worker data)
   # annotationXaXscore_workers: key: annotation ID -> value: (key: annotator -> value: score)
   # hXa_workers: key: HIT ID -> value: (key: annotator -> value: data line)
   # The first two dictionaries will help us the workers' annotations, and the 
   # latter four dictionaries will store all the data we need in organized formats.
   def read_data_worker_only(self, worker_filename):
      infile_workers = open(self.input_dir + "/" + worker_filename)
      reader_workers = csv.reader(infile_workers, quotechar='"')

      # Read in the AMT worker data.
      self.annotationXhit_workers = {}
      self.annotationXaXscore_workers = {}
      self.annotatorXannotationXscore_workers = {}
      self.hXa_workers = {}
      self.worker_list = []
      row_num = 0
      for row in reader_workers:
         if row_num > 0:
            annotator = row[self.columnXid_workers["WorkerId"]]
            hit_id = row[self.columnXid_workers["HITId"]]
            assignment_status = row[self.columnXid_workers["AssignmentStatus"]]
            self.start_idx = self.columnXid_workers["Answer.0metaphorNovelty0"]
            self.end_idx = self.columnXid_workers["Answer.example1"]

            # In some rare cases, we may reverse someone's rejection.  We still
            # don't want to use their annotations in this case (reason for
            # reversal is usually an email from that annotator).  We can
            # identify these cases as those that have values in both the
            # "ApprovalTime" and "RejectionTime" columns.
            if row[self.columnXid_workers["ApprovalTime"]].strip() != "" and row[self.columnXid_workers["RejectionTime"]].strip() != "":
               assignment_status = "Rejected"

            # Don't count annotations from rejected HITs.  Also, some header
            # rows are interspersed throughout combined_hits.csv.  Obviously,
            # these shouldn't be counted as annotations; they can be identified
            # by checking to see if the annotator ID is "WorkerId" (the name of
            # that column).
            if assignment_status != "Rejected" and annotator != "WorkerId":
               if hit_id not in self.hXa_workers:
                  self.hXa_workers[hit_id] = {}
               self.hXa_workers[hit_id][annotator] = row
               self.worker_list.append(annotator)

               # Now, break this row apart into individual annotations to populate
               # self.annotationXhit_workers.
               for x in range(self.start_idx, self.end_idx):
                  if row[x].strip() != "":
                     parts = row[x].split()
                     annotation = parts[0]  # The identifier for the instance being annotated.
                     score = int(parts[1])
                     self.annotationXhit_workers[annotation] = hit_id

                     if annotation not in self.annotationXaXscore_workers:
                        self.annotationXaXscore_workers[annotation] = {}
                     self.annotationXaXscore_workers[annotation][annotator] = score

                     # Also index each annotator by annotation and that annotation's score.
                     if annotator not in self.annotatorXannotationXscore_workers:
                        self.annotatorXannotationXscore_workers[annotator] = {}
                     self.annotatorXannotationXscore_workers[annotator][annotation] = score
         else:  # Store the indices associated with each column; makes indexing more transparent in the future.
            self.columnXid_workers = {}
            col_num = 0
            for column in row:
               self.columnXid_workers[column] = col_num
               col_num += 1
         row_num += 1


   # Read the data into several dictionaries:
   # annotationXhit_workers: key: annotation ID -> value: HIT ID (worker data)
   # annotationXhit_expert: key: annotation ID -> value: HIT ID (expert data)
   # annotationXaXscore_workers: key: annotation ID -> value: (key: annotator -> value: score)
   # annotationXaXscore_expert: key: annotation ID -> value: (key: annotator -> value: score)
   # hXa_workers: key: HIT ID -> value: (key: annotator -> value: data line)
   # hXa_expert: key: HIT ID -> value: (key: annotator -> value: data line)
   # The first two dictionaries will help us merge the expert's and the workers'
   # annotations, and the latter four dictionaries will store all the data we
   # need in organized formats.
   def read_data(self, worker_filename, expert_filename):
      infile_workers = open(self.input_dir + "/" + worker_filename)
      infile_expert = open(self.input_dir + "/" + expert_filename)

      reader_workers = csv.reader(infile_workers, quotechar='"')
      reader_expert = csv.reader(infile_expert, quotechar='"')

      # First read in the AMT worker data.
      self.annotationXhit_workers = {}
      self.annotationXaXscore_workers = {}
      self.annotatorXannotationXscore_workers = {}
      self.hXa_workers = {}
      self.worker_list = []
      row_num = 0
      for row in reader_workers:
         if row_num > 0:
            annotator = row[self.columnXid_workers["WorkerId"]]
            hit_id = row[self.columnXid_workers["HITId"]]
            assignment_status = row[self.columnXid_workers["AssignmentStatus"]]
            self.start_idx = self.columnXid_workers["Answer.0metaphorNovelty0"]
            self.end_idx = self.columnXid_workers["Answer.example1"]

            # In some rare cases, we may reverse someone's rejection.  We still
            # don't want to use their annotations in this case (reason for
            # reversal is usually an email from that annotator).  We can
            # identify these cases as those that have values in both the
            # "ApprovalTime" and "RejectionTime" columns.
            if row[self.columnXid_workers["ApprovalTime"]].strip() != "" and row[self.columnXid_workers["RejectionTime"]].strip() != "":
               assignment_status = "Rejected"

            # Don't count annotations from rejected HITs.  Also, some header
            # rows are interspersed throughout combined_hits.csv.  Obviously,
            # these shouldn't be counted as annotations; they can be identified
            # by checking to see if the annotator ID is "WorkerId" (the name of
            # that column).
            if assignment_status != "Rejected" and annotator != "WorkerId":
               if hit_id not in self.hXa_workers:
                  self.hXa_workers[hit_id] = {}
               self.hXa_workers[hit_id][annotator] = row
               self.worker_list.append(annotator)

               # Now, break this row apart into individual annotations to populate
               # self.annotationXhit_workers.
               for x in range(self.start_idx, self.end_idx):
                  if row[x].strip() != "":
                     parts = row[x].split()
                     annotation = parts[0]  # The identifier for the instance being annotated.
                     score = int(parts[1])
                     self.annotationXhit_workers[annotation] = hit_id

                     if annotation not in self.annotationXaXscore_workers:
                        self.annotationXaXscore_workers[annotation] = {}
                     self.annotationXaXscore_workers[annotation][annotator] = score

                     # Also index each annotator by annotation and that annotation's score.
                     if annotator not in self.annotatorXannotationXscore_workers:
                        self.annotatorXannotationXscore_workers[annotator] = {}
                     self.annotatorXannotationXscore_workers[annotator][annotation] = score
         else:  # Store the indices associated with each column; makes indexing more transparent in the future.
            self.columnXid_workers = {}
            col_num = 0
            for column in row:
               self.columnXid_workers[column] = col_num
               col_num += 1
         row_num += 1

      # Next, read in the expert's data.
      self.annotationXhit_expert = {}
      self.annotationXaXscore_expert = {}
      self.hXa_expert = {}
      row_num = 0
      for row in reader_expert:
         if row_num > 0:
            annotator = row[self.columnXid_expert["WorkerId"]]
            hit_id = row[self.columnXid_expert["HITId"]]
            assignment_status = row[self.columnXid_expert["AssignmentStatus"]]
            self.start_idx = self.columnXid_expert["Answer.0metaphorNovelty0"]
            self.end_idx = self.columnXid_expert["Answer.example1"]

            # Some header rows are interspersed throughout.  Obviously,
            # these shouldn't be counted as annotations; they can be identified
            # by checking to see if the annotator ID is "WorkerId" (the name of
            # that column).
            if assignment_status != "Rejected" and annotator != "WorkerId":
               if hit_id not in self.hXa_expert:
                  self.hXa_expert[hit_id] = {}
               self.hXa_expert[hit_id][annotator] = row

               # Now, break this row apart into individual annotations to populate
               # self.annotationXhit_expert.
               for x in range(self.start_idx, self.end_idx):
                  if row[x].strip() != "":
                     parts = row[x].split()
                     annotation = parts[0]  # The identifier for the instance being annotated.
                     score = int(parts[1])
                     self.annotationXhit_expert[annotation] = hit_id

                     if annotation not in self.annotationXaXscore_expert:
                        self.annotationXaXscore_expert[annotation] = {}
                     self.annotationXaXscore_expert[annotation][annotator] = score
         else:  # Store the indices associated with each column.
            self.columnXid_expert = {}
            col_num = 0
            for column in row:
               self.columnXid_expert[column] = col_num
               col_num += 1
         row_num += 1

   # Read the data into several dictionaries:
   # annotationXhit_workers: key: annotation ID -> value: HIT ID (worker data)
   # annotationXhit_expert: key: annotation ID -> value: HIT ID (expert data)
   # annotationXaXscore_workers: key: annotation ID -> value: (key: annotator -> value: score)
   # annotationXaXscore_expert: key: annotation ID -> value: (key: annotator -> value: score)
   # hXa_workers: key: HIT ID -> value: (key: annotator -> value: data line)
   # hXa_expert: key: HIT ID -> value: (key: annotator -> value: data line)
   # These are the four dictionaries used by our algorithm when using our dataset.
   # We need to put the Snow data in this format for easy integration with the rest
   # of the code.  Columns from the Snow dataset will be assigned as follows:
   # !amt_annotation_ids: not used
   # !amt_worker_ids: annotator (gold->expert)
   # orig_id: annotation ID and HIT ID
   # score: response (AMT worker) or gold (which we're referring to as expert).
   def read_data_snow(self, worker_filename):
      infile_workers = open(self.input_dir + "/" + worker_filename)
      reader_workers = csv.reader(infile_workers, delimiter="\t", quotechar='"')

      # First read in the AMT worker data.
      self.annotationXhit_workers = {}
      self.annotationXaXscore_workers = {}
      self.annotatorXannotationXscore_workers = {}
      self.hXa_workers = {}
      self.worker_list = []

      self.annotationXhit_expert = {}
      self.annotationXaXscore_expert = {}
      self.hXa_expert = {}
      row_num = 0
      for row in reader_workers:
         if row_num > 0:
            annotator = row[self.columnXid_workers["!amt_worker_ids"]]
            hit_id = row[self.columnXid_workers["orig_id"]]  # Assume each headline is its own HIT.
            annotation = row[self.columnXid_workers["orig_id"]]
            score = float(row[self.columnXid_workers["response"]])
            gold = float(row[self.columnXid_workers["gold"]])

            if hit_id not in self.hXa_workers:
               self.hXa_workers[hit_id] = {}
            self.hXa_workers[hit_id][annotator] = row
            self.worker_list.append(annotator)
            if hit_id not in self.hXa_expert:
               self.hXa_expert[hit_id] = {}
            self.hXa_expert[hit_id][annotator] = row

            # Index each annotation ID by annotator.
            if annotation not in self.annotationXaXscore_workers:
               self.annotationXaXscore_workers[annotation] = {}
            self.annotationXaXscore_workers[annotation][annotator] = score

            if annotation not in self.annotationXaXscore_expert:
               self.annotationXaXscore_expert[annotation] = {}
            self.annotationXaXscore_expert[annotation]["gold"] = gold

            # Also index each annotator by annotation and that annotation's score.
            if annotator not in self.annotatorXannotationXscore_workers:
               self.annotatorXannotationXscore_workers[annotator] = {}
            self.annotatorXannotationXscore_workers[annotator][annotation] = score
         else:  # Store the indices associated with each column.
            self.columnXid_workers = {}
            col_num = 0
            for column in row:
               self.columnXid_workers[column] = col_num
               col_num += 1
         row_num += 1
      self.worker_list = list(set(self.worker_list))


   # Read the data into several dictionaries:
   # annotationXhit_workers: key: annotation ID -> value: HIT ID (worker data)
   # annotationXhit_expert: key: annotation ID -> value: HIT ID (expert data)
   # annotationXaXscore_workers: key: annotation ID -> value: (key: annotator -> value: score)
   # annotationXaXscore_expert: key: annotation ID -> value: (key: annotator -> value: score)
   # hXa_workers: key: HIT ID -> value: (key: annotator -> value: data line)
   # hXa_expert: key: HIT ID -> value: (key: annotator -> value: data line)
   # These are the four dictionaries used by our algorithm when using our dataset.
   # We need to put the TREC data in this format for easy integration with the rest
   # of the code.  Columns from the TREC dataset will be assigned as follows:
   # workerID: annotator (gold->expert)
   # topicID_docID: annotation ID
   # score: label or gold (which we're referring to as expert).
   def read_data_trec(self, worker_filename):
      infile_workers = open(self.input_dir + "/" + worker_filename)
      reader_workers = csv.reader(infile_workers, delimiter="\t", quotechar='"')

      # First read in the AMT worker data.
      self.annotationXhit_workers = {}
      self.annotationXaXscore_workers = {}
      self.annotatorXannotationXscore_workers = {}
      self.hXa_workers = {}
      self.worker_list = []

      self.annotationXhit_expert = {}
      self.annotationXaXscore_expert = {}
      self.hXa_expert = {}
      row_num = 0
      for row in reader_workers:
         if row_num > 0:
            annotator = row[self.columnXid_workers["workerID"]]
            annotation = row[self.columnXid_workers["topicID"]]+"_"+row[self.columnXid_workers["docID"]]  # Assume each headline is its own HIT.
            hit_id = row[self.columnXid_workers["hitID"]]
            score = float(row[self.columnXid_workers["label"]])
            gold = float(row[self.columnXid_workers["gold"]])

            # Continue past any lines with a gold value of -1 (no gold label) or -2 (broken link).
            if gold < 0 or score < 0:
               row_num += 1
               continue

            if hit_id not in self.hXa_workers:
               self.hXa_workers[hit_id] = {}
            self.hXa_workers[hit_id][annotator] = row
            self.worker_list.append(annotator)
            if hit_id not in self.hXa_expert:
               self.hXa_expert[hit_id] = {}
            self.hXa_expert[hit_id][annotator] = row

            # Index each annotation ID by annotator.
            if annotation not in self.annotationXaXscore_workers:
               self.annotationXaXscore_workers[annotation] = {}
            self.annotationXaXscore_workers[annotation][annotator] = score

            if annotation not in self.annotationXaXscore_expert:
               self.annotationXaXscore_expert[annotation] = {}
            self.annotationXaXscore_expert[annotation]["gold"] = gold

            # Also index each annotator by annotation and that annotation's score.
            if annotator not in self.annotatorXannotationXscore_workers:
               self.annotatorXannotationXscore_workers[annotator] = {}
            self.annotatorXannotationXscore_workers[annotator][annotation] = score
         else:  # Store the indices associated with each column; makes indexing more transparent in the future.
            self.columnXid_workers = {}
            col_num = 0
            for column in row:
               self.columnXid_workers[column] = col_num
               col_num += 1
         row_num += 1
      self.worker_list = list(set(self.worker_list))


   # Create a list of the workers who have an average r-value, across all HITs that
   # they completed, greater than 0.35.
   def identify_good_annotators(self):
      good_annotators = []
      annotatorXr_j = {}  # key: annotator -> value: the annotator's average r-value with everyone in PGA.
      a_jXa_superscript_j = defaultdict(list)  # key: a_j -> value: the annotator's A^{j} list.
      self.a_jXa_kXr_jk = {}
      for a_j in self.annotatorXannotationXscore_workers:  # For each annotator a_j in A.
         # Set A^{j} = the subset of PGA who annotated at least one unfiltered HIT h 
         # in common with a_j.
         a_superscript_j = []
         for annotator in self.annotatorXannotationXscore_workers:
            if annotator != a_j:  # We don't want or need the annotator's correlation with him/herself.
               for h in self.hXa_workers:
                  # Check to see if (a) both a_j and this member of PGA annotated this HIT,
                  # and further that (b) this HIT was not filtered for either annotator.
                  if a_j in self.hXa_workers[h] and annotator in self.hXa_workers[h]:
                     a_superscript_j.append(annotator)
         a_superscript_j = list(set(a_superscript_j))
         a_jXa_superscript_j[a_j] = a_superscript_j

         r_jk_sum = 0.0
         r_jk_num = 0.0
         self.a_jXa_kXr_jk[a_j] = {}
         for a_k in a_superscript_j:  # For each annotator a_k in A^{j}.
            # Compute the correlation r_j,k between a_j and a_k, over unfiltered HITs.
            a_j_scores = []
            a_k_scores = []
            for annotation in self.annotationXaXscore_workers:
               if a_j in self.annotationXaXscore_workers[annotation] and a_k in self.annotationXaXscore_workers[annotation]:
                  a_j_scores.append(self.annotationXaXscore_workers[annotation][a_j])
                  a_k_scores.append(self.annotationXaXscore_workers[annotation][a_k])

            # Compute the correlation coefficient.
            if len(a_j_scores) > 0:
               slope, intercept, r_jk, p_value, std_err = scipy.stats.linregress(a_j_scores, a_k_scores)
               r_jk_sum += r_jk  # The sum of all the r-values so far.
               r_jk_num += 1.0  # The number of r-values we've summed together.
               self.a_jXa_kXr_jk[a_j][a_k] = r_jk  # Store this for later.

         # Compute the average correlation, r_j, between a_j and all a_k in A^{j}.
         if r_jk_num > 0:
            r_j = r_jk_sum / r_jk_num
            annotatorXr_j[a_j] = r_j

      # Now that we have all of the annotators' average r-values, identifying the
      # good annotators is trivial.
      for a_j in annotatorXr_j:
         if annotatorXr_j[a_j] > 0.35:
            good_annotators.append(a_j)
      return good_annotators

   # For the instance given, find and return its five annotators with the highest 
   # average R-values across all HITs they completed, along with a list of those
   # annotators' average r-values, respectively.  Additionally, return a dictionary
   # of those annotators' average r-values only with good annotators, respectively
   # (unless they had no overlaps with good annotators, in which case just return
   # their regular average r-values).
   def select_best_annotators(self, instance, good_annotators, num_best=5):
      best_annotators = []
      best_annotators_with_good_annotators = []
      avg_r_values_good = {}
      if len(self.annotationXaXscore_workers[instance]) < num_best:
      #   print "Not enough annotations for: ", instance
      #   print "Repost HIT: ", self.annotationXhit_workers[instance]
         pass
      else:
         # Find the five annotators with the highest everage R-values across
         # all HITs they completed.
         avg_r_values = {}
         for a_j in self.annotationXaXscore_workers[instance]:
            # Build a list of all of the workers with whom this worker's annotations
            # overlapped.
            overlapping_workers = []
            for h in self.hXa_workers:
               if a_j in self.hXa_workers[h]:
                  for a_k in self.hXa_workers[h]:
                     if a_j != a_k:
                        overlapping_workers.append(a_k)
            overlapping_workers = list(set(overlapping_workers))  # Remove duplicates.

            # Get this annotator's r-value with each worker with whom they had
            # an overlapping annotation.
            r_value_with = {}
            r_value_with_good = {}
            for a_k in overlapping_workers:
               a_j_annotations = []
               a_k_annotations = []
               for annotation in self.annotationXaXscore_workers:
                  if a_j in self.annotationXaXscore_workers[annotation] and a_k in self.annotationXaXscore_workers[annotation]:
                     a_j_annotations.append(self.annotationXaXscore_workers[annotation][a_j])
                     a_k_annotations.append(self.annotationXaXscore_workers[annotation][a_k])
               slope, intercept, r_jk, p_value, std_err = scipy.stats.linregress(a_j_annotations, a_k_annotations)
               r_value_with[a_k] = r_jk
               if a_k in good_annotators:
                  r_value_with_good[a_k] = r_jk

            # Compute the average r-value for a_j using the individual r-values
            # computed between a_j and each worker with whom they overlapped.
            r_sum = 0.0
            r_count = 0.0
            for a_k in r_value_with:
               r_sum += r_value_with[a_k]
               r_count += 1.0
            avg_r_values[a_j] = r_sum / r_count

            # Compute the average r-value for a_j compared only to overlapping
            # good annotators.
            r_sum = 0.0
            r_count = 0.0
            for a_k in r_value_with_good:
               r_sum += r_value_with_good[a_k]
               r_count += 1.0
            if r_count > 0.0:
               avg_r_values_good[a_j] = r_sum / r_count

         # Sort the average r-values from highest to lowest.
         sorted_avg_r_values = sorted(avg_r_values.items(), key=operator.itemgetter(1))[::-1]

         # Add the five annotators with the highest average r-values to the list
         # of best annotators.
         for a_j, r in sorted_avg_r_values:
            best_annotators.append((a_j, r))
            if len(best_annotators) == num_best:
               break
      return best_annotators, avg_r_values_good

   # Compute the average r-value for a given instance, by finding the r-value between
   # each possible pair between the five best annotators for the HIT, on that HIT
   # only.  Weight the r-value between a pair of annotators by the average of their
   # average r-values.  Then sum all of the weighted r-values, and divide by the
   # sum of the weights.
   def compute_weighted_avg_r_HIT(self, instance, best_annotators):
      weighted_r_sum = 0.0
      weight_sum = 0.0
      pair_r_values = []
      for a_i, r_i in best_annotators:
         for a_j, r_j in best_annotators:
            if a_i != a_j and (a_i+"_"+a_j) not in pair_r_values and (a_j+"_"+a_i) not in pair_r_values:
               # If we've made it this deep into the loop, we have found a pair for which we have not already computed an r-value.
               hit_id = self.annotationXhit_workers[instance]
               row_i = self.hXa_workers[hit_id][a_i]
               row_j = self.hXa_workers[hit_id][a_j]
               annotations_i = []
               annotations_j = []

               # Get all the annotations for this HIT.
               for x in range(self.start_idx, self.end_idx):
                  if row_i[x].strip() != "" and row_j[x].strip() != "":
                     annotations_i.append(int(row_i[x].split()[1]))
                     annotations_j.append(int(row_j[x].split()[1]))

               # Compute R between the two annotators, compute the weight between them, and update appropriate sums.
               slope, intercept, r_ij, p_value, std_err = scipy.stats.linregress(annotations_i, annotations_j)
               weight = ((r_i + r_j) / 2)
               weighted_r_sum += weight * r_ij
               weight_sum += weight
               pair_r_values.append(a_i+"_"+a_j)
      weighted_avg_r = weighted_r_sum / weight_sum
      return weighted_avg_r

   # Compute the average r-value for a given instance, by finding the r-value between
   # each possible pair between the annotators for the HIT, on that HIT only.  
   # Weight the r-value between a pair of annotators by the average of their
   # average r-values.  Then sum all of the weighted r-values, and divide by the
   # sum of the weights.
   def compute_weighted_avg_r_HIT_snow(self, instance, best_annotators):
      weighted_r_sum = 0.0
      r_sum = 0.0
      weight_sum = 0.0
      count = 0.0
      pair_r_values = []
      for a_i, r_i in best_annotators:
         for a_j, r_j in best_annotators:
            if a_i != a_j and (a_i+"_"+a_j) not in pair_r_values and (a_j+"_"+a_i) not in pair_r_values:
               # If we've made it this deep into the loop, we have found a pair for which we have not already computed an r-value.
               hit_id = self.annotationXhit_workers[instance]
               annotations_i = []
               annotations_j = []

               # Get all the annotations for this HIT.
               for annotation in self.annotationXhit_workers:
             #     if instance in self.annotationXhit_workers[annotation]:  # Uncomment for Affect dataset.
                  if hit_id == self.annotationXhit_workers[annotation] and a_i in self.annotationXaXscore_workers[annotation] and a_j in self.annotationXaXscore_workers[annotation]:  # Uncomment for TREC dataset.
                     annotations_i.append(self.annotationXaXscore_workers[annotation][a_i])
                     annotations_j.append(self.annotationXaXscore_workers[annotation][a_j])

               # Compute R between the two annotators, compute the weight between them, and update appropriate sums.
               slope, intercept, r_ij, p_value, std_err = scipy.stats.linregress(annotations_i, annotations_j)
               weight = ((r_i + r_j) / 2)
               weighted_r_sum += weight * r_ij
               r_sum += r_ij
               weight_sum += weight
               count += 1
               pair_r_values.append(a_i+"_"+a_j)
      weighted_avg_r = weighted_r_sum / weight_sum
      avg_r = r_sum / count
      if avg_r != 0.0:
         print "Avg_R: ", avg_r
      return weighted_avg_r
      

   # For each instance, create a feature vector.  Current features include:
   # - Includes the "true" label for the instance (last column), if labeled=True
   def get_feature_vector(self, instance, good_annotators, num_best=5, labeled=False):
      fv = []
      fv.append(instance.replace("'", ""))  # Weka is weird about quote characters.
      best_annotators, avg_r_values_good = self.select_best_annotators(instance, good_annotators, num_best)  # 5 annotations with the highest average r-value, if more than five workers annotated this instance.
      if len(best_annotators) < num_best:
         return []

      # Get all of the annotations received for the instance.
      annotations = []
      for a_j, r in best_annotators:
         annotations.append((self.annotationXaXscore_workers[instance][a_j], r, a_j))
      sorted_annotations = sorted(annotations, key=lambda x: x[0])[::-1]  # Sort by score, then reverse the list of tuples so it's from highest to lowest.
      sorted_annotations_by_r = sorted(annotations, key=lambda x: x[1])[::-1]  # Sort by average r-value, then reverse the list of tuples so it's from highest to lowest.

      # Break the list of tuples into two lists to easily compute highest, lowest, and average annotations.
      scores,r_values,annotators = zip(*sorted_annotations)

      # Get the average annotation score for the instance.
      avg_score = sum(scores) / float(len(scores))
      fv.append(avg_score)

      # Get the weighted average annotation score for the instance.
      weighted_scores = [score*r for score,r,annotator in sorted_annotations]
      weighted_avg_score = sum(weighted_scores) / sum(r_values)
      fv.append(weighted_avg_score)

      # Get the weighted average annotation score for the instance, using the average
      # r-values for the annotators only with other good annotators as the weights.
      weighted_score_sum = 0
      weight_sum = 0
      for score,r,annotator in sorted_annotations:
         if annotator in avg_r_values_good:
            weighted_score_sum += (score * avg_r_values_good[annotator])
            weight_sum += avg_r_values_good[annotator]
         else:
            weighted_score_sum += (score * r)
            weight_sum += r
      weighted_avg_score = weighted_score_sum / weight_sum
      fv.append(weighted_avg_score)

      # Get the average r-value for this HIT.
      weighted_r_HIT = self.compute_weighted_avg_r_HIT(instance, best_annotators)
      fv.append(weighted_r_HIT)

      # Add the annotations and respective annotators' r-values to the feature 
      # vector in order from highest score to lowest.
      for a, r, a_j in sorted_annotations:
         fv.append(a)
         fv.append(r)

         # Add the annotator's average r-value only with good annotators, or
         # repeat the earlier r-value if unavailable.
         if a_j in avg_r_values_good:
            fv.append(avg_r_values_good[a_j])
         else:
            fv.append(r)

      # Add the annotations and respective annotators' r-values to the feature
      # vector in order from highest average r to lowest.
      for a, r, a_j in sorted_annotations_by_r:
         fv.append(a)
         fv.append(r)

      if labeled:
         # Get the true label for the instance (my label).
         for a_j in self.annotationXaXscore_expert[instance]:
            fv.append(self.annotationXaXscore_expert[instance][a_j])
      else:
         fv.append("?")

      return fv

   # Get a special feature vector that includes a column for each annotator in the dataset.
   def get_mace_vector(self, instance, good_annotators, num_best=5):
      fv = []
      fv.append(instance.replace("'", ""))  # Instance ID, for internal reference.
      best_annotators, avg_r_values_good = self.select_best_annotators(instance, good_annotators, num_best)  # 5 annotations with the highest average r-value, if more than five workers annotated this instance.
      
      best_annotator_ids = [a_j for a_j, r in best_annotators]
      fv = []
      for worker in self.worker_list:
         if worker in best_annotator_ids:
            fv.append(self.annotationXaXscore_workers[instance][worker])
         else:
            fv.append(None)
      return fv

   # Create a data file for the instances, formatted as a CSV file with one
   # instance per line.  Also split the data randomly into three groups, and
   # create a file for each.
   def create_data_files(self, good_annotators, num_best=5, labeled="True", prefix=""):
      train_outfile = open(self.output_dir + "/" + prefix+"_label_dataset_train.csv", "w")
      validation_outfile = open(self.output_dir + "/" + prefix+"_label_dataset_validation.csv", "w")
      test_outfile = open(self.output_dir + "/" + prefix+"_label_dataset_test.csv", "w")
      mace_train_out = open(self.output_dir + "/" + prefix+"_train_mace_vector.csv", "w")
      mace_validation_out = open(self.output_dir + "/" + prefix+"_validation_mace_vector.csv", "w")
      mace_test_out = open(self.output_dir + "/" + prefix+"_test_mace_vector.csv", "w")

      train_writer = csv.writer(train_outfile, quotechar='"')
      validation_writer = csv.writer(validation_outfile, quotechar='"')
      test_writer = csv.writer(test_outfile, quotechar='"')
      mace_train = csv.writer(mace_train_out)
      mace_validation = csv.writer(mace_validation_out)
      mace_test = csv.writer(mace_test_out)

      outfile = open(self.output_dir + "/" + prefix+"_label_dataset.csv", "w")
      writer = csv.writer(outfile, quotechar='"')

      # Write a header line.
      labels = ["ID", "Avg_Annotation", "Weighted_Avg_Annotation", "Weighted_Avg_Annotation_Good", "Weighted_R_HIT", "A1", "A1_R", "A1_R_Good", "A2", "A2_R", "A2_R_Good", "A3", "A3_R", "A3_R_Good", "A4", "A4_R", "A4_R_Good", "A5", "A5_R", "A5_R_Good", "A1xR", "A1xR_R", "A2xR", "A2xR_R", "A3xR", "A3xR_R", "A4xR", "A4xR_R", "A5xR", "A5xR_R", "True_Label"]  # Uncomment for WebRel and our dataset.
   #   labels = ["ID", "Avg_Annotation", "Weighted_Avg_Annotation", "Weighted_Avg_Annotation_Good", "Weighted_R_HIT", "A1", "A1_R", "A1_R_Good", "A2", "A2_R", "A2_R_Good", "A3", "A3_R", "A3_R_Good", "A4", "A4_R", "A4_R_Good", "A5", "A5_R", "A5_R_Good", "A6", "A6_R", "A6_R_Good", "A7", "A7_R", "A7_R_Good", "A8", "A8_R", "A8_R_Good", "A9", "A9_R", "A9_R_Good", "A10", "A10_R", "A10_R_Good", "A1xR", "A1xR_R", "A2xR", "A2xR_R", "A3xR", "A3xR_R", "A4xR", "A4xR_R", "A5xR", "A5xR_R", "A6xR", "A6xR_R", "A7xR", "A7xR_R", "A8xR", "A8xR_R", "A9xR", "A9xR_R", "A10xR", "A10xR_R", "True_Label"]  # Uncomment for Affect datasets.
      train_writer.writerow(labels)
      validation_writer.writerow(labels)
      test_writer.writerow(labels)
      writer.writerow(labels)

      # Loop through each instance.
      random.seed(1)
      train_count = 0
      validation_count = 0
      test_count = 0
      for annotation in self.annotationXaXscore_workers:
         if ((train_count + validation_count + test_count) % 100) == 0:
            print "Instance #:", (train_count + validation_count + test_count)
         # Get a random number; if the instance count associated with that "number" 
         # is greater than numInstances / 3, repeat.
         while True:
            random_num = random.randint(1,3)
            if random_num == 1 and train_count < ((len(self.annotationXaXscore_workers) / 3) + 1):
               break
            elif random_num == 2 and validation_count < ((len(self.annotationXaXscore_workers) / 3) + 1):
               break
            elif random_num == 3 and test_count < ((len(self.annotationXaXscore_workers) / 3) + 1):
               break
         fv = self.get_feature_vector(annotation, good_annotators, num_best, labeled)
         if len(fv) == 0:
            continue
         mace_fv = self.get_mace_vector(annotation, good_annotators, num_best)
         writer.writerow(fv)
         if random_num == 1:
            train_writer.writerow(fv)
            mace_train.writerow(mace_fv)
            train_count += 1
         elif random_num == 2:
            validation_writer.writerow(fv)
            mace_validation.writerow(mace_fv)
            validation_count += 1
         elif random_num == 3:
            test_writer.writerow(fv)
            mace_test.writerow(mace_fv)
            test_count += 1
      outfile.close()
      train_outfile.close()
      validation_outfile.close()
      test_outfile.close()
      mace_train_out.close()
      mace_validation_out.close()
      mace_test_out.close()

   # Create an unlabeled data file for the instances, formatted as a CSV file with one
   # instance per line.
   def create_unlabeled_data_file(self, good_annotators, fold_num):
      outfile = open(self.output_dir + "/unlabeled_crowdsourced_instances.csv", "w")
      writer = csv.writer(outfile, quotechar='"')

      # Write a header line.
      labels = ["ID", "Avg_Annotation", "Weighted_Avg_Annotation", "Weighted_Avg_Annotation_Good", "Weighted_R_HIT", "A1", "A1_R", "A1_R_Good", "A2", "A2_R", "A2_R_Good", "A3", "A3_R", "A3_R_Good", "A4", "A4_R", "A4_R_Good", "A5", "A5_R", "A5_R_Good", "A1xR", "A1xR_R", "A2xR", "A2xR_R", "A3xR", "A3xR_R", "A4xR", "A4xR_R", "A5xR", "A5xR_R", "True_Label"]
      writer.writerow(labels)

      # Loop through each instance.
      for annotation in self.annotationXaXscore_workers:
         if len(self.annotationXaXscore_workers[annotation]) >= 5:
            fv = self.get_feature_vector(annotation, good_annotators)
            writer.writerow(fv)
         else:
            print "Not enough instances for " + annotation + "; repost its HIT (" + self.annotationXhit_workers[annotation] + ")!"
      outfile.close()

   def Main(self):
      self.input_dir = "sample_input"
      self.output_dir = "sample_output"
      worker_filename = "anonymized_combined_hits_1-100.csv"
      expert_filename = "expert_combined_hits_1-100.csv"

      self.read_data(worker_filename, expert_filename)
    #  self.read_data_worker_only(worker_filename)  # Use read_data(worker, expert) if you also have adjudications for this data.
    #  self.read_data_snow(worker_filename)  # Use to read data from Affect (Emotion) or Affect (Valence).
    #  self.read_data_trec(worker_filename)  # Use to read data from WebRel.
      good_annotators = self.identify_good_annotators()
      self.create_data_files(good_annotators, num_best=5, labeled=True, prefix="sample")  # Set num_best to the number of annotations you wish to use for each instance.
    #  self.create_unlabeled_data_file(good_annotators, 5)

if __name__ == '__main__':
   adjudication_instance_builder = AdjudicationInstanceBuilder()
   adjudication_instance_builder.Main()
