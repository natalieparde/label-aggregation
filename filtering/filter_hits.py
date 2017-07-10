###############################################################################
# filter_hits.py
#
# Natalie Parde
# 4/12/2017
#
# Process the HITs in the specified file according to the HIT Annotation
# Algorithm as follows:
#
# Post the i-th set of hits H_i = {h_i,1, h_i,2, ... h_i,m}
# Let H be the set of all annotated hits: H = Union H_j=1..i
# Let A_i = {a_i,1, a_i,2, ... a_i,n}, be the set of annotators who annotated H_i
# Let A be the set of all annotators: A = Union A_j=1..i  (regardless of the annotators quality)
# Let BR be a set of annotators considered for any reason to be Bad Robots (described later)
# Let A*_i be the subset of A who were not deemed to be in BR following the processing of H_i-1
#
# Perform the current annotator-based quality analysis, but include all annotators, A:
# Filter out bad annotators and bad hit annotations
# Initialize the set of Potentially Good Annotators (PGA) = A*_i - filtered(A_i)
# Until stopping criteria is met
#    For each annotator a_j in A
#       Set A^(j) = the subset of PGA who annotated at least one unfiltered hit h in common with a_j
#       ((For each annotator a_k in A^(j)
#          Compute the correlation r_j,k between a_j and a_k, over unfiltered hits  ))
#          Compute the average correlation, r_j, between a_j and all a_k in A^(j)
#    Set B = the subset of A with r_j < 0.0; B' = the subset of A with r_j == 0.0; 
#    B'' = the subset of A, of size |B|, with the lowest r_j > 0.0; and B''' = the subset of A with r_j < 0.1
#    Set PGA = A - (B + B' + B'' + B''' + filtered(A))
#
# Set GA (Good Annotators) = the subset of A with r_j > 0.35
# Set BR (Bad Robots) = B + B' + bottom(round(2/3|B|), B'') + filtered(A)  (but not including a_j where A^(j) = empty set)
# Set UQA (Unknown Quality Annotators) = A - BR - GA
#
# Add all a_j in BR to the disqualified list
# Remove all a_j in BR from the double-pay list
# Add all a_j in UQA who annotated at least two hits and who had r'_j < 0.1 to the disqualified list
# Remove all a_j in UQA who had r'_j < 0.25 from the double-pay list
# Reject all hit annotations from BR that hadn't previously been paid
# Reject all filtered hits
# Accept all GA and UQA hits that weren't filtered
# Add all a_j in GA who annotated at least three hits to the double-pay list
#
# For hits that have only been posted once, and that have no outstanding rejections / requests for annotation
# If at most one GA annotated the hit or if only two did and their correlation is under 0.3 on this hit, repost the hit
#
###############################################################################

import csv
import operator
import scipy.stats
from collections import defaultdict

class HITProcessor:

   # Read in data from the list of filenames specified.  Store the data in
   # several formats:
   # annotationXannotatorXscore: key: annotation ID -> value: (key: annotator -> value: score)
   # annotatorXannotationXscore: key: annotator -> value: (key: annotation ID -> value: score)
   # annotatorXannotation_iXscore: Key: annotator -> value: (key: annotation ID -> value: score)  This
   #    set is limited to only those HITs that have not yet been accepted or rejected (H_i).
   # annotationXhit: key: annotation ID -> value: HIT ID
   # hitXannotatorXdata: key: HIT ID -> value: (key: annotator -> value: full line of data)
   # columnXid: key: column name -> value: column index
   # hit_iXannotatorXdata: HIT ID -> value: (key: annotator -> value: full line of data)  This
   #    set is limited to only those HITs that have not yet been accepted or rejected.
   # annotatorXhitXscoresXcount: key: annotator -> value: (key: HIT ID -> value: (key: score -> value: count))
   # hitXannotatorXtime: key: HIT ID -> value: (key: annotator -> value: time in seconds)
   # annotators_i: list of the annotators who annotated H_i.
   def read_hit_data(self, filenames):
      for filename in filenames:
         print "Reading file: " + filename
         infile = open(self.input_dir + filename)
         reader = csv.reader(infile, quotechar='"')
         self.annotationXannotatorXscore = {}
         self.annotatorXannotationXscore = {}
         self.annotatorXannotation_iXscore = {}
         self.annotationXhit = defaultdict(list)
         self.hitXannotatorXdata = {}
         self.hit_iXannotatorXdata = {}
         self.annotators_i = []
         self.annotatorXhitXscoresXcount = {}
         self.hitXannotatorXtime = {}
         self.already_rejected = []

         # Process the file row by row, ignoring the first row.
         row_num = 0
         status_changed = []
         for row in reader:
            if row_num > 0:
               annotator = row[self.columnXid["WorkerId"]]
               hit_id = row[self.columnXid["HITId"]]
               assignment_status = row[self.columnXid["AssignmentStatus"]]
               if annotator != "WorkerId":
                  time = int(row[self.columnXid["WorkTimeInSeconds"]])
               self.start_idx = self.columnXid["Answer.0metaphorNovelty0"]
               self.end_idx = self.columnXid["Answer.example1"]

               # In some rare cases, we may reverse someone's rejection.  We still
               # don't want to use their annotations in this case (reason for
               # reversal is usually an email from that annotator).  We can
               # identify these cases as those that have values in both the
               # "ApprovalTime" and "RejectionTime" columns.
               if row[self.columnXid["ApprovalTime"]].strip() != "" and row[self.columnXid["RejectionTime"]].strip():
                  if annotator != "WorkerId":
                     status_changed.append(hit_id + " " + annotator)
                  assignment_status = "Rejected"

               # Only look at rows that have not already been rejected.
               if assignment_status != "Rejected" and annotator != "WorkerId":
                   # Add the entire row of data, indexed by HIT ID and annotator.
                   if hit_id not in self.hitXannotatorXdata:
                      self.hitXannotatorXdata[hit_id] = defaultdict(list)
                   self.hitXannotatorXdata[hit_id][annotator] = row

                   # If this row of data has never previously been accepted or 
                   # rejected, add it to the list of new HITs, H_i.
                   if assignment_status == "Submitted":
                      if hit_id not in self.hit_iXannotatorXdata:
                         self.hit_iXannotatorXdata[hit_id] = defaultdict(list)
                      self.hit_iXannotatorXdata[hit_id][annotator] = row
                      self.annotators_i.append(annotator)

                   # Store the time (in seconds) that it took this annotator to
                   # complete this HIT.
                   if hit_id not in self.hitXannotatorXtime:
                      self.hitXannotatorXtime[hit_id] = {}
                   self.hitXannotatorXtime[hit_id][annotator] = time

                   # Loop through each of the annotations associated with this HIT.
                   for i in range(self.start_idx, self.end_idx):
                      if row[i].strip() != "":
                         anno_parts = row[i].split()  # The annotation ID and score are whitespace-separated.
                         annotation = anno_parts[0]
                         score = anno_parts[1]

                         # Index each annotation by annotator and that annotator's score.
                         # If by some small chance this annotator has completed this
                         # annotation twice (e.g., in a reposted HIT), the second row
                         # of data will overwrite the first, so we won't have duplicate
                         # annotations by the same annotator for the same HIT ever.
                         if annotation not in self.annotationXannotatorXscore:
                            self.annotationXannotatorXscore[annotation] = {}
                         self.annotationXannotatorXscore[annotation][annotator] = score

                         # Also index each annotator by annotation and that annotation's score.
                         if annotator not in self.annotatorXannotationXscore:
                            self.annotatorXannotationXscore[annotator] = {}
                         self.annotatorXannotationXscore[annotator][annotation] = score

                         # Do the same thing, but only count annotations that have been
                         # made in this round (H_i).
                         if assignment_status == "Submitted":
                            if annotator not in self.annotatorXannotation_iXscore:
                               self.annotatorXannotation_iXscore[annotator] = {}
                            self.annotatorXannotation_iXscore[annotator][annotation] = score

                         # Update the list holding score counts for each possible score
                         # (0-3) assigned during this HIT, by this annotator.
                         if annotator not in self.annotatorXhitXscoresXcount:
                            self.annotatorXhitXscoresXcount[annotator] = {}
                         if hit_id not in self.annotatorXhitXscoresXcount[annotator]:
                            self.annotatorXhitXscoresXcount[annotator][hit_id] = {}
                         if score not in self.annotatorXhitXscoresXcount[annotator][hit_id]:
                            self.annotatorXhitXscoresXcount[annotator][hit_id][score] = 0
                         self.annotatorXhitXscoresXcount[annotator][hit_id][score] += 1

                         # Finally, if the annotation hasn't already been indexed by HIT, do so now.
                         # Since reposts of HITs may have different HIT IDs attached to annotations,
                         # let each annotation have a list of associated HIT IDs.
                         if annotation not in self.annotationXhit:
                            self.annotationXhit[annotation] = []
                         if hit_id not in self.annotationXhit[annotation]:
                            self.annotationXhit[annotation].append(hit_id)
               else:
                  self.already_rejected.append(annotator+"_"+hit_id)
            else:  # Store the indices associated with each column; makes indexing more transparent in the future.
               self.columnXid = {}
               col_num = 0
               for column in row:
                  self.columnXid[column] = col_num
                  col_num += 1
            row_num += 1
         infile.close()

         print "The following HITs were accepted at a later time after first being rejected (generally due to emails from the workers).  We still internally consider them to be rejected:"
         for assignment in status_changed:
            parts = assignment.split()
            hit_id = parts[0]
            worker = parts[1]
            print "HIT " + hit_id + " from Worker " + worker

   # Filter out the annotators who, for all of their HITs, annotated
   # the entire HIT with the same score.
   # Filter out the HITs that were completed too quickly (< 80 seconds).
   # Return the lists of filtered annotators and filtered HITsXannotator, where:
   #    key: HIT ID -> value: list of filtered annotators associated with that HIT ID.
   def get_filtered_Annotators(self, only_hit_i=False):
      print "Filtering annotators and HITs."
      self.filtered_Annotators = []
      filtered_hitsXannotator = defaultdict(list)
      valid_annotators = list(self.annotatorXannotationXscore.keys())
      if only_hit_i:
         valid_annotators = list(set(self.annotators_i))
         print "Only considering annotators from H_i."

      # Find the annotators who, for at least round(half) of their HITs, annotated
      # the entire HIT with the same score.
      for annotator in self.annotatorXhitXscoresXcount:
         if annotator in valid_annotators:
            num_hits = len(self.annotatorXhitXscoresXcount[annotator])
            num_hits_same_score_throughout = 0  # Initialize the count to zero.

            # Determine how many HITs from this annotator were annotated with the
            # same score throughout.
            for hit_id in self.annotatorXhitXscoresXcount[annotator]:
               if len(self.annotatorXhitXscoresXcount[annotator][hit_id]) == 1:
                  num_hits_same_score_throughout += 1

            # Check to see if this was too many according to our criteria.
            if num_hits_same_score_throughout == num_hits:
               self.filtered_Annotators.append(annotator)

      # Find the HITs that were completed too quickly.
      for hit_id in self.hitXannotatorXtime:
         for annotator in self.hitXannotatorXtime[hit_id]:
            if self.hitXannotatorXtime[hit_id][annotator] < 80:
               if hit_id not in filtered_hitsXannotator:
                  filtered_hitsXannotator[hit_id] = []
               filtered_hitsXannotator[hit_id].append(annotator)
      return self.filtered_Annotators, filtered_hitsXannotator

   # Perform an annotator-based quality analysis on the HITs, including all
   # annotators (not just those who have completed annotations in this "round"
   # of HITs).  See algorithm specified earlier for details.
   def perform_quality_analysis(self, filtered_A, filtered_A_i, filtered_hitsXannotator, previous_BR):
      print "Performing quality analysis."
      astar_i = []  # Subset of A who were not in BR following the processing of H_{i-1} (the last "round" of HITs).
      for annotator in self.annotatorXannotationXscore:
         if annotator not in previous_BR:
            astar_i.append(annotator)

      PGA = []  # Potentially good annotators initialized to A*_i - filtered(A_i)
      for annotator in astar_i:
         if annotator not in self.filtered_A_i:
            PGA.append(annotator)

      annotatorXr_j = {}  # Declare this variable outside of the loop since we need to use it after the loop terminates.
      a_jXa_superscript_j = defaultdict(list)  # Same.
      B = []
      B_prime = []
      B_prime_prime = []
      B_prime_prime_prime = []
      last_PGA = astar_i  # We'll stop at convergence or after 10 iterations.  So, we'll start by initializing last_pga to basically anyone who wasn't bad in the last round.
      iterations = 0  # We'll also force the process to iterate at least once, in case no annotators were filtered (see above initialization).
      while iterations == 0 or (PGA != last_PGA and iterations < 10):  # Until stopping criteria is met:
         # Create an output file showing details from the analysis for each iteration.
         outfile = open(self.output_dir + self.filename.replace(".csv", "") + "_qa_stage" + str(iterations) + ".tsv", "w")
         writer = csv.writer(outfile, delimiter="\t", quoting=csv.QUOTE_NONNUMERIC)
         writer.writerow(["avg_r", "#hitsAntd", "totalNumAnnotations", "#hitsAntdH_i", "numAnnotationsH_i", "csvListOfPairedRs", "scoreDistribution", "annotatorId"])

         # Set the new last_PGA to the current PGA, since at the end of this iteration we'll
         # recompute the current PGA.
         last_PGA = PGA

         annotatorXr_j = {}  # key: annotator -> value: the annotator's average r-value with everyone in PGA.
         a_jXa_superscript_j = defaultdict(list)  # key: a_j -> value: the annotator's A^{j} list.
         self.a_jXa_kXr_jk = {}
         for a_j in self.annotatorXannotationXscore:  # For each annotator a_j in A.
            # Set A^{j} = the subset of PGA who annotated at least one unfiltered HIT h 
            # in common with a_j.
            a_superscript_j = []
            for annotator in PGA:
               if annotator != a_j:  # We don't want or need the annotator's correlation with him/herself.
                  for h in self.hitXannotatorXdata:
                     # Check to see if (a) both a_j and this member of PGA annotated this HIT,
                     # and further that (b) this HIT was not filtered for either annotator.
                     if (a_j in self.hitXannotatorXdata[h] and annotator in self.hitXannotatorXdata[h] 
                        and a_j not in filtered_hitsXannotator[h] and annotator not in filtered_hitsXannotator[h]):
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
               for annotation in self.annotationXannotatorXscore:
                  if a_j in self.annotationXannotatorXscore[annotation] and a_k in self.annotationXannotatorXscore[annotation]:
                     # HITs will have different HIT IDs when reposted.  This 
                     # ensures that we're looking at the right HIT ID for each 
                     # annotator, for this annotation.
                     hits_associated_with_this_annotation = self.annotationXhit[annotation]
                     hit_id_aj = ""  
                     hit_id_ak = ""
                     for h in hits_associated_with_this_annotation:
                        if a_j in self.hitXannotatorXdata[h]:
                           hit_id_aj = h
                        if a_k in self.hitXannotatorXdata[h]:
                           hit_id_ak = h

                     # Make sure this HIT wasn't filtered.  If it wasn't add the
                     # scores given by the annotators to their respective score lists.
                     if (a_j not in filtered_hitsXannotator[hit_id_aj] 
                        and a_k not in filtered_hitsXannotator[hit_id_ak]):
                           a_j_scores.append(int(self.annotationXannotatorXscore[annotation][a_j]))
                           a_k_scores.append(int(self.annotationXannotatorXscore[annotation][a_k]))

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

         # Set B = the subset of A with r_j < 0.0
         # Set B' = the subset of A with r_j == 0.0
         # Set B'' = the subset of A, of size |B|, with the lowest r_j > 0.0
         # Set B''' = the subset of A with r_j < 0.1
         B = []
         B_prime = []
         B_prime_prime = []
         B_prime_prime_prime = []
         self.sorted_annotatorXr_j = sorted(annotatorXr_j.items(), key=operator.itemgetter(1))
         positive_count = 0
         for a_j, r_j in self.sorted_annotatorXr_j:
            if r_j < 0.0:
               B.append(a_j)
            if r_j == 0.0:
               B_prime.append(a_j)
            if r_j < 0.1:
               B_prime_prime_prime.append(a_j)
         for a_j, r_j in self.sorted_annotatorXr_j:
            if r_j > 0.0 and positive_count < len(B):
               B_prime_prime.append(a_j)
               positive_count += 1

         # Set PGA = A - (B + B' + B'' + B''' + filtered(A))
         PGA = []
         for a_j in self.annotatorXannotationXscore:  # The list of annotators in self.annotatorXannotatorXscore is equivalent to A.
            if a_j not in B:
               if a_j not in B_prime:
                  if a_j not in B_prime_prime:
                     if a_j not in B_prime_prime_prime:
                        if a_j not in self.filtered_A:
                           PGA.append(a_j)
         # Uncomment the line below if you would like to view the PGA list for each iteration.
         # print "\nIteration " + str(iterations) + " PGA:\n" + "\n".join(PGA)

         # Output the data analysis from this iteration.
         for a_j in self.annotatorXannotationXscore:  # For each annotator a_j in A.
            writer.writerow(self.get_stats(a_j))

         iterations += 1
         outfile.close()

      # After termination (either due to convergence or 10 iterations).
      # Set GA (Good Annotators) = the subset of A with r_j > 0.35.
      GA = []
      for a_j, r_j in self.sorted_annotatorXr_j:
         if r_j > 0.35:
            GA.append(a_j)

      # Set BR (Bad Robots) = B + B' + bottom(round(2/3)|B|),B'') + filtered(A), not including a_j where A^{j} is an empty set.
      bottom_num = round((2.0/3.0)*len(B))
      bottom = []
      bottom_counter = 0
      for b in B_prime_prime:
         if bottom_counter < bottom_num:
            bottom.append(b)
         bottom_counter += 1

      BR = B + B_prime + bottom
      for a_j in self.filtered_A:
         if len(a_jXa_superscript_j[a_j]) > 0:
            BR.append(a_j)
      BR = list(set(BR))  # I don't think there should ever be duplicates in this, but good to double-check I guess.

      # Set UQA (Unknown Quality Annotators) = A - BR - GA.
      UQA = []
      for a_j in self.annotatorXannotationXscore:
         if a_j not in BR and a_j not in GA:
            UQA.append(a_j)

      print "\nBR:\n" + "\n".join(BR)
      print "\nGA:\n" + "\n".join(GA)
      print "\nUQA:\n" + "\n".join(UQA)

      # Remove all a_j in BR from the double-pay list.
      current_double_pays = self.get_double_pays()
      remove_double_pay = []
      for a_j in list(BR):
         if a_j in current_double_pays:
            remove_double_pay.append(a_j)
      
      # Remove all a_j in UQA who had r'_j < 0.25 from the double-pay list.
      for a_j in UQA:
         for annotator, r in self.sorted_annotatorXr_j:
            if a_j == annotator:
               if r < 0.25:
                  if a_j in current_double_pays:
                     remove_double_pay.append(a_j)
      if len(remove_double_pay) > 0:
         print "\nRemove the following annotators from the double-pay list:\n" + "\n".join(remove_double_pay)

      # Reject all HIT annotations from BR that haven't been accepted or rejected yet.
      self.reject_hit_ids = []
      self.reject_pairs = []  # List of annotator_hitID pairs to be rejected.
      print "\nReject the HIT annotations from BR that haven't been accepted or rejected yet:"
      reject_br_hits = defaultdict(list)
      for a_j in BR:
         for hit_id in self.hit_iXannotatorXdata:
            if a_j in self.hit_iXannotatorXdata[hit_id]:
               if a_j not in reject_br_hits:
                  reject_br_hits[a_j] = []
               reject_br_hits[a_j].append(hit_id)
               self.reject_hit_ids.append(hit_id)
               self.reject_pairs.append(a_j+"_"+hit_id)
         if len(reject_br_hits[a_j]) > 0:
            print "Reject the following HITs from Worker " + a_j + ":"
            for hit_id in reject_br_hits[a_j]:
               print "HIT " + hit_id

      # Reject all filtered HITs.
      print "\nReject all HITs (that have not yet been accepted) from the following filtered annotators:", self.filtered_A
      print "Specifically, reject:"
      reject_filtered_hits = defaultdict(list)
      for a_j in self.filtered_A:
         for hit_id in self.hit_iXannotatorXdata:
            if a_j in self.hit_iXannotatorXdata[hit_id]:
               if a_j not in reject_filtered_hits:
                  reject_filtered_hits[a_j] = []
               reject_filtered_hits[a_j].append(hit_id)
               self.reject_hit_ids.append(hit_id)
               self.reject_pairs.append(a_j+"_"+hit_id)
         print "From Worker " + a_j + ":"
         for hit_id in reject_filtered_hits[a_j]:
            print "HIT " + hit_id

      print "\nReject the following specific HITs (not necessarily all from the annotator):"
      for hit_id in filtered_hitsXannotator:
         for a_j in filtered_hitsXannotator[hit_id]:
            if hit_id in self.hit_iXannotatorXdata and a_j in self.hit_iXannotatorXdata[hit_id]:
                print "Worker " + a_j + "\tHIT " + hit_id
                self.reject_hit_ids.append(hit_id)
                self.reject_pairs.append(a_j+"_"+hit_id)
            
      # Add all a_j in BR to the disqualified list.
      no_fly = BR
      
      # Add all a_j in UQA who annotated at least two HITs and who had r'_j < 0.1
      # to the disqualified list.
      for a_j in UQA:
         for annotator, r in self.sorted_annotatorXr_j:
            if a_j == annotator:
               if r < 0.1:
                  if len(self.annotatorXhitXscoresXcount[a_j]) >= 2:
                     no_fly.append(a_j)
                     
      # Reject people who have no unfiltered HITs and add them to the disqualified list.
      num_filtered = {}  # key:worker -> value:number of filtered HITs for that worker.
      for hit_id in filtered_hitsXannotator:
         for a_j in filtered_hitsXannotator[hit_id]:
            if a_j not in num_filtered:
               num_filtered[a_j] = 0
            num_filtered[a_j] += 1
            
      # Check to see if the number of filtered HITs for this worker is equal to their total number of HITs.
      for a_j in num_filtered:
         if num_filtered[a_j] == len(self.annotatorXhitXscoresXcount[a_j]):
            no_fly.append(a_j)

      # Also output just those who are new to the disqualified list, so they can be easily
      # added.  Add any new members to a list maintained in a text document.
      existing_nofly_file = open(self.output_dir + "disqualified_list.txt", "a+")
      existing_nofly_list = []
      for line in existing_nofly_file:
         existing_nofly_list.append(line.strip())

      print "\nAdd the following annotators to the disqualified list:"
      for a_i in no_fly:
         if a_i not in existing_nofly_list:
            print a_i
            existing_nofly_file.write(a_i + "\n")
      existing_nofly_file.close()

      # Accept all GA and UQA HITs that weren't filtered.
      print "\nAccept the following HITs:"
      self.accept_pairs = []  # List of annotator_hitID pairs to be accepted.
      accept_hits = defaultdict(list)
      for hit_id in self.hit_iXannotatorXdata:
         for a_j in self.hit_iXannotatorXdata[hit_id]:
            if (a_j in GA or a_j in UQA) and a_j not in self.filtered_A and (a_j+"_"+hit_id) not in self.reject_pairs:
               if a_j not in accept_hits:
                  accept_hits[a_j] = []
               accept_hits[a_j].append(hit_id)
               self.accept_pairs.append(a_j+"_"+hit_id)
      for a_j in accept_hits:
         print "From Worker " + a_j + ":"
         for hit_id in accept_hits[a_j]:
            print "HIT " + hit_id

      # Add all a_j in GA who annotated at least three HITs to the double-pay list.
      add_double_pay = []
      for a_j in GA:
         if len(self.annotatorXhitXscoresXcount[a_j]) >= 3:
            add_double_pay.append(a_j)

      # Also output just those who are new to the double-pay list, so they can be 
      # easily added.  Add any new members to a list maintained in a text document.
      existing_doublepay_file = open(self.output_dir + "double-pay_list.txt", "a+")
      existing_doublepay_list = []
      for line in existing_doublepay_file:
         existing_doublepay_list.append(line.strip())

      print "\nAdd the following annotators to the double-pay list:"
      for a_i in add_double_pay:
         if a_i not in existing_doublepay_list:
            print a_i
            existing_doublepay_file.write(a_i + "\n")
      existing_doublepay_file.close()

      """
      # Uncomment if you want to repost HITs with few good annotators.
      # For HITs that have only been posted once, and that have no outstanding
      # rejections / requests for annotation, if at most one GA annotated the
      # HIT or if only two did and their correlation is under 0.3 on this HIT,
      # repost the HIT.
      possible_reposts = []
      already_reposted = self.get_reposts()  # HITs can only be reposted once.
      for hit_id in self.hitXannotatorXdata:
         if hit_id not in already_reposted:
            outstanding_rejection = False
            for pair in self.reject_pairs:
               parts = pair.split("_")
               if hit_id == parts[1]:
                  outstanding_rejection = True

            if not outstanding_rejection:
               possible_reposts.append(hit_id)

      # If at most one GA annotated the HIT or if only two did and their
      # correlation is under 0.3 on this HIT, repost the HIT.
      self.reposts = []
      print len(possible_reposts), "possible reposts."
      for hit_id in possible_reposts:
         GAs_who_annotated_this_hit = []
         for a_j in self.hitXannotatorXdata[hit_id]:
            if a_j in GA:
               GAs_who_annotated_this_hit.append(a_j)

         if len(GAs_who_annotated_this_hit) <= 1:  # If at most one GA annotated the HIT
            print "Reposting because one or fewer GAs annotated this HIT."
            self.reposts.append(hit_id)
         elif len(GAs_who_annotated_this_hit) == 2:  # Or only two did
            a0 = self.hitXannotatorXdata[hit_id][GAs_who_annotated_this_hit[0]]
            a1 = self.hitXannotatorXdata[hit_id][GAs_who_annotated_this_hit[1]]
            a0_avg_r = self.annotatorXr_j[GAs_who_annotated_this_hit[0]]
            a1_avg_r = self.annotatorXr_j[GAs_who_annotated_this_hit[1]]
            weight = (a0_avg_r + a1_avg_r) / 2.0

            # Compute the r-value between the two on just this HIT.
            a0_annotations = []
            a1_annotations = []
            for i in range(self.start_idx, self.end_idx):
               if a0[i].strip() != "" and a1[i].strip() != "":
                  a0_parts = a0[i].split()
                  a1_parts = a1[i].split()

                  # We don't really need to keep the annotation labels here; just the scores for computing r-values.
                  a0_annotations.append(int(a0_parts[1]))
                  a1_annotations.append(int(a1_parts[1]))
            slope, intercept, r_01, p_value, std_err = scipy.stats.linregress(a0_annotations, a1_annotations)
            print "Weighted R for HIT " + hit_id + " between only GAs is: " + str(weight * r_01)
            if (weight * r_01) < 0.3:  # If their correlation is under 0.3 on this HIT
               self.reposts.append(hit_id)  # Repost the HIT.
         else:  # If any number of GAs annotated the HIT and the average R for the HIT is < 0.2, repost.
            weight_sum = 0.0
            weighted_r_sum = 0.0
            for a_j in self.hitXannotatorXdata[hit_id]:
               for a_k in self.hitXannotatorXdata[hit_id]:
                  if a_j != a_k:
                     a0 = self.hitXannotatorXdata[hit_id][a_j]
                     a1 = self.hitXannotatorXdata[hit_id][a_k]
                     a0_avg_r = self.annotatorXr_j[a_j]
                     a1_avg_r = self.annotatorXr_j[a_k]
                     weight = (a0_avg_r + a1_avg_r) / 2.0
                     weight_sum += weight

                     # Compute the r-value between the two on just this HIT.
                     a0_annotations = []
                     a1_annotations = []
                     for i in range(self.start_idx, self.end_idx):
                        if a0[i].strip() != "" and a1[i].strip() != "":
                           a0_parts = a0[i].split()
                           a1_parts = a1[i].split()

                           # We don't really need to keep the annotation labels here; just the scores for computing r-values.
                           a0_annotations.append(int(a0_parts[1]))
                           a1_annotations.append(int(a1_parts[1]))
                     slope, intercept, r_01, p_value, std_err = scipy.stats.linregress(a0_annotations, a1_annotations)
                     weighted_r_sum += (weight * r_01)
            avg_weighted_r = weighted_r_sum / weight_sum
            print "Avg. Weighted R for HIT " + hit_id + " is: " + str(avg_weighted_r)
            if avg_weighted_r < 0.2:  # If the average correlation, weighted toward "better" annotators, is less than 0.2 on this HIT, repost it.
               self.reposts.append(hit_id)  # Repost the HIT.
      print "\nRepost the following HITs:"
      if len(self.reposts) == 0:
         print "(None right now.)"
      else:
         for hit_id in self.reposts:
            print "HIT " + hit_id
      """      

   # Get the following data for the specified worker:
   # - avg_r with PGA
   # - # HITs annotated
   # - totalNumAnnotations
   # - # HITs annotated this round
   # - numAnnotationsThisRound
   # - csvListOfPairedRs
   # - annotatorId
   # - scoreDistribution
   def get_stats(self, worker):
      csv_elements = []

      # avg_r with PGA in most recent iteration.
      found_worker = False
      for a_j, r_j in self.sorted_annotatorXr_j:
         if a_j == worker:
            found_worker = True
            csv_elements.append(round(r_j, 4))
            self.annotatorXr_j[a_j] = r_j
      if not found_worker:
         csv_elements.append("N/A")

      # # HITs annotated by this worker.
      num_hits = 0
      for hit_id in self.hitXannotatorXdata:
         if worker in self.hitXannotatorXdata[hit_id]:
            num_hits += 1
      csv_elements.append(num_hits)

      # total number of annotations from this worker.
      num_annotations = len(self.annotatorXannotationXscore[worker])
      csv_elements.append(num_annotations)

      # # HITs annotated during this round (H_i).
      num_hits_h_i = 0
      for hit_id in self.hit_iXannotatorXdata:
         if worker in self.hit_iXannotatorXdata[hit_id]:
            num_hits_h_i += 1
      csv_elements.append(num_hits_h_i)

      # number of annotations from this worker, during this round (H_i).
      num_annotations_h_i = 0
      if worker in self.annotatorXannotation_iXscore:
         num_annotations_h_i = len(self.annotatorXannotation_iXscore[worker])
      csv_elements.append(num_annotations_h_i)

      # CSV list of paired r-values.
      paired_rs = ""
      if worker in self.a_jXa_kXr_jk:
         for a_k in self.a_jXa_kXr_jk[worker]:
            paired_rs += a_k + "=" + str(round(self.a_jXa_kXr_jk[worker][a_k], 4)) + ", "
         paired_rs = paired_rs.strip().strip(",")
      if paired_rs == "":
         paired_rs = "No overlapping annotators in PGA."
      csv_elements.append(paired_rs)

      # score distribution for this worker
      num0 = 0.0
      num1 = 0.0
      num2 = 0.0
      num3 = 0.0
      for hit_id in self.annotatorXhitXscoresXcount[worker]:
         if "0" in self.annotatorXhitXscoresXcount[worker][hit_id]:
            num0 += self.annotatorXhitXscoresXcount[worker][hit_id]["0"]
         if "1" in self.annotatorXhitXscoresXcount[worker][hit_id]:
            num1 += self.annotatorXhitXscoresXcount[worker][hit_id]["1"]
         if "2" in self.annotatorXhitXscoresXcount[worker][hit_id]:
            num2 += self.annotatorXhitXscoresXcount[worker][hit_id]["2"]
         if "3" in self.annotatorXhitXscoresXcount[worker][hit_id]:
            num3 += self.annotatorXhitXscoresXcount[worker][hit_id]["3"]
      num_scores = num0 + num1 + num2 + num3
      percent0 = round((num0/num_scores)*100, 1)
      percent1 = round((num1/num_scores)*100, 1)
      percent2 = round((num2/num_scores)*100, 1)
      percent3 = round((num3/num_scores)*100, 1)
      distribution = "0=" + str(percent0) + "%, 1=" + str(percent1) + "%, 2=" + str(percent2) + "%, 3=" + str(percent3) + "%"
      csv_elements.append(distribution)

      # the worker's ID
      csv_elements.append(worker)

      return csv_elements
      
   # Print out list that contains, for each HIT with no outstanding annotations: 
   # a. The HIT ID
   # b. The correlation coefficient between each pair of annotations on that HIT, 
   # weighted by the two annotators' average avg_r values, so if avg_r for a_1 is 
   # .52, and avg_r for a_2 is .32, and the the r-value between a_1 and a_2 on 
   # HIT 5 only is .47, then the weighted r-value for that pair, for HIT 5 is 
   # ((.52 + .32) / 2) * .47, where ((.52 + .32) / 2) is the weight.
   # c. Then figure out the weighted average pairwise r-value for the HIT by 
   # summing the weighted r-values between each pair for that HIT, and then 
   # dividing by the sum of the weights.
   def get_hit_correlations(self, hit_id):
      output_string = ""
      output_string += hit_id + "\t"
      
      # Get all possible annotator pairs for this HIT.
      possible_pairs = []
      for a_j in self.hitXannotatorXdata[hit_id]:
        for a_k in self.hitXannotatorXdata[hit_id]:
            if a_j != a_k:
                if (a_j + "_" + a_k) not in possible_pairs and (a_k + "_" + a_j) not in possible_pairs:
                    possible_pairs.append(a_j + "_" + a_k)
                    
      # Get the correlation coefficient between each pair, for only this HIT.
      pair_correlations = ""
      weighted_pair_correlations = ""
      weight_sum = 0.0
      r_sum = 0.0
      unweighted_r_sum = 0.0
      r_num = 0.0
      for pair in possible_pairs:
         annotators = pair.split("_")
         a_1 = annotators[0]
         a_2 = annotators[1]
         a1_avg_r = self.annotatorXr_j[a_1]
         a2_avg_r = self.annotatorXr_j[a_2]
         weight = (a1_avg_r + a2_avg_r) / 2.0
         weight_sum += weight
         
         # Loop through each of the annotations associated with this HIT.
         a1_annotations = []
         a2_annotations = []
         for i in range(self.start_idx, self.end_idx):
            if self.hitXannotatorXdata[hit_id][a_1][i].strip() != "":
               if self.hitXannotatorXdata[hit_id][a_2][i].strip() != "":
                  anno_parts1 = self.hitXannotatorXdata[hit_id][a_1][i].split()  # The annotation ID and score are whitespace-separated.
                  score = anno_parts1[1]
                  a1_annotations.append(int(score))
                  
                  anno_parts2 = self.hitXannotatorXdata[hit_id][a_2][i].split()  # The annotation ID and score are whitespace-separated.
                  score = anno_parts2[1]
                  a2_annotations.append(int(score))

         # Get the correlation coefficient for this pair.
         slope, intercept, r_jk, p_value, std_err = scipy.stats.linregress(a1_annotations, a2_annotations)         
         pair_correlations += "Worker " + a_1 + " and Worker " + a_2 + " R-Value: " + str(r_jk) + ", "
         weighted_pair_correlations += "Worker " + a_1 + " and Worker " + a_2 + " Weighted R-Value: " + str(r_jk * weight) + ", "
         r_sum += (r_jk * weight)
         unweighted_r_sum += r_jk
         r_num += 1
      pair_correlations = pair_correlations.strip().strip(",")
      output_string += pair_correlations + "\t"
      
      weighted_pair_correlations = weighted_pair_correlations.strip().strip(",")
      output_string += weighted_pair_correlations + "\t"
      
      # Then figure out the weighted average pairwise r-value for the HIT by 
      # summing the weighted r-values between each pair for that HIT, and then 
      # dividing by the sum of the weights.
      weighted_avg = r_sum / weight_sum
      unweighted_avg = unweighted_r_sum / r_num
      output_string += str(weighted_avg) + "\t" + str(unweighted_avg)

      return output_string


   # Reject all HITs indicated according to the algorithm, and accept all
   # GA and UQA HITs that were not rejected.
   def accept_and_reject_hits(self, filename):
      infile = open(self.input_dir + filename)
      outfile = open(self.output_dir + "processed_" + filename, "w")

      reader = csv.reader(infile, quotechar='"')
      writer = csv.writer(outfile, quotechar='"')

      for row in reader:
         a_j = row[self.columnXid["WorkerId"]]
         hit_id = row[self.columnXid["HITId"]]
         assignment_status = row[self.columnXid["AssignmentStatus"]]

         # Check rejected pairs (first, in case a filtered assignment was included
         # when computing UQA and GA), then accepted pairs (all remaining UQA and GA),
         # and modify the CSV file as necessary.
         if assignment_status == "Submitted" and (a_j+"_"+hit_id) in self.reject_pairs:  # Modify the file so that this assignment is rejected.
            row.append("")
            row.append("Auto-Reject: Quality-control program identified this assignment as spam.")
            writer.writerow(row)
         elif assignment_status == "Submitted" and (a_j+"_"+hit_id) in self.accept_pairs:  # Modify the file so that this assignment is accepted.
            row.append("x")
            writer.writerow(row)
         else:  # Do nothing; output as-is.  This should only be the header row.
            writer.writerow(row)
      outfile.close()
      infile.close()

   # Get an annotator's average correlation coefficient for each HIT.  Print
   # out the number of times the r-value is > 0, equal to 0, and < 0.  Also
   # print the annotator's average R-value with other workers, and their average
   # HIT correlation score.
   def get_annotatorXhit_correlations(self, a_j):
      info_lines = []
      if a_j not in self.a_jXhitXa_kXr:
         self.a_jXhitXa_kXr[a_j] = {}
      for hit_id in self.hitXannotatorXdata:
         if hit_id in self.hitXannotatorXdata and a_j in self.hitXannotatorXdata[hit_id]:  # If a_j annotated this HIT.
            if (a_j+"_"+hit_id) not in self.already_rejected:  # If a_j's annotation of the HIT hasn't already been rejected.
               if hit_id not in self.a_jXhitXa_kXr[a_j]:
                  self.a_jXhitXa_kXr[a_j][hit_id] = {}
               for a_k in self.hitXannotatorXdata[hit_id]:  # For all other annotators who annotated this HIT.
                  if a_j != a_k:  # Don't want the annotator's r-value with him/herself!
                     # Loop through each of the annotations associated with this HIT.
                     a_j_annotations = []
                     a_k_annotations = []
                     for i in range(self.start_idx, self.end_idx):
                        if self.hitXannotatorXdata[hit_id][a_j][i].strip() != "" and self.hitXannotatorXdata[hit_id][a_k][i].strip() != "":
                           a_j_annotations.append(int(self.hitXannotatorXdata[hit_id][a_j][i].split()[1]))
                           a_k_annotations.append(int(self.hitXannotatorXdata[hit_id][a_k][i].split()[1]))
                     # Get the correlation coefficient for this pair.
                     slope, intercept, r_jk, p_value, std_err = scipy.stats.linregress(a_j_annotations, a_k_annotations)  
                     self.a_jXhitXa_kXr[a_j][hit_id][a_k] = r_jk
         # For this HIT, get the # times R > 0, # times R = 0, # times R < 0, and average R.
         if hit_id in self.a_jXhitXa_kXr[a_j]:
            num_gt_0 = 0.0
            num_eq_0 = 0.0
            num_lt_0 = 0.0
            r_sum = 0.0
            r_num = 0.0
            for a_k in self.a_jXhitXa_kXr[a_j][hit_id]:
               if self.a_jXhitXa_kXr[a_j][hit_id][a_k] > 0:
                  num_gt_0 += 1
               elif self.a_jXhitXa_kXr[a_j][hit_id][a_k] == 0:
                  num_eq_0 += 1
               elif self.a_jXhitXa_kXr[a_j][hit_id][a_k] < 0:
                  num_lt_0 += 1
               r_sum += self.a_jXhitXa_kXr[a_j][hit_id][a_k]
               r_num += 1
            r_avg = r_sum / r_num
            # Print this information to a string.
            info_lines.append([hit_id, str(num_gt_0), str(num_eq_0), str(num_lt_0), str(r_avg)])
      return info_lines

   # Maintain a list of HIT IDs that have already been reposted.
   def update_repost_list(self):
      existing_reposts = self.get_reposts()
      repost_file = open(self.output_dir + "amt_reposts.txt", "a")
      for repost in self.reposts:
         if repost not in existing_reposts:
            repost_file.write(repost + "\n")
      repost_file.close()

   # Read in the list of HIT IDs that have already been reposted.
   def get_reposts(self):
      reposts = []
      try:
         repost_file = open(self.output_dir + "amt_reposts.txt")
         for line in repost_file:
            reposts.append(line.strip())
         repost_file.close()
      except IOError:
         print "No reposts yet!"
      return reposts

   # Read in the list of workers already in the double-pay list.
   def get_double_pays(self):
      double_pays = []
      try:
         double_pay_file = open(self.output_dir + "double-pay_list.txt")
         for line in double_pay_file:
            double_pays.append(line.strip())
         double_pay_file.close()
      except IOError:
         # print "No double-pays yet!"
         pass
      return double_pays

   def Main(self):
      self.annotatorXr_j = {}
      self.input_dir = "sample_input/"
      self.output_dir = "sample_output/"
      self.filename = "anonymized_combined_hits_1-100.csv"
      new_hits_filename = "anonymized_new_hits_91-100.csv"
      self.read_hit_data([self.filename])
      self.filtered_A, filtered_hitsXannotator = self.get_filtered_Annotators()
      self.filtered_A_i, filtered_hits_iXannotator = self.get_filtered_Annotators(only_hit_i=True)
      self.perform_quality_analysis(self.filtered_A, self.filtered_A_i, filtered_hitsXannotator, [])  # previous_BR set to empty list because we don't have a previous BR at the moment.
      self.accept_and_reject_hits(new_hits_filename)
    #  self.update_repost_list()  # Uncomment if you want to repost HITs that didn't have many good annotators.

      # Print out the weighted correlation coefficient information.
      print "\n\nGathering r-values for HITs with no outstanding reposts",
      r_info_file = open(self.output_dir + "r_value_info.tsv", "w")
      writer = csv.writer(r_info_file, delimiter="\t")
      writer.writerow(["HIT ID", "Pairwise R, Unweighted", "Pairwise R, Weighted", "Average Pairwise R, Weighted"])
      for hit_id in self.hitXannotatorXdata:
         # If reposting some HITs due to having few good workers, change the line below to:
         # if hit_id not in self.reject_hit_ids and hit_id not in self.reposts:
         if hit_id not in self.reject_hit_ids:
            # print "Checking HIT " + hit_id
            print ".",
            writer.writerow(self.get_hit_correlations(hit_id).strip().split("\t"))
      r_info_file.close()

      # Print out the HIT-wise r-value info for each annotator.
      self.a_jXhitXa_kXr = {}
      print "\nPrinting HIT-wise r-value info for each annotator."
      hit_r_file = open(self.output_dir + "hitwise_r_info.tsv", "w")
      writer = csv.writer(hit_r_file, delimiter="\t")
      for a_j in self.annotatorXannotationXscore:
         hit_r_file.write(a_j + "\n")
         writer.writerow(["HIT ID", "# R>0", "# R=0", "# R<0", "Avg. R"])
         info_lines = self.get_annotatorXhit_correlations(a_j)
         for line in info_lines:
            writer.writerow(line)
         hit_r_file.write("\n")
      hit_r_file.close()

      print"\n\n****************************************************************\n"
      print "All finished!  You can upload the following file to Amazon Mechanical Turk to accept and reject your HITs: processed_" + new_hits_filename

if __name__ == '__main__':
   hit_processor = HITProcessor()
   hit_processor.Main()
