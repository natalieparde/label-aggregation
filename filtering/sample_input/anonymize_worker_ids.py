###############################################################################
#
# anonymize_worker_ids.py
#
# Natalie Parde
# 7/10/2017
#
# Script used to anonymize AMT worker IDs before releasing the data publicly.
#
###############################################################################

import csv

class AnonymizeWorkerIDs:

   # Read all of the worker IDs in one or more files, and store them to a
   # dictionary as follows:
   # key: worker_id -> value: integer (incremented by 1 for each worker ID
   # added to the dictionary).
   def read_data(self, filenames):
      self.worker_ids = {}
      worker_counter = 1

      # Loop through each file.
      for filename in filenames:
         line_counter = 0
         infile = open(filename)
         reader = csv.reader(infile, quotechar='"')
         for row in reader:
            if line_counter > 0:

               # Add this worker ID to the dictionary (unless it's from a
               # header row, e.g., "WorkerId").
               worker = row[self.columnXid["WorkerId"]]
               if worker != "WorkerId" and worker not in self.worker_ids:
                  self.worker_ids[worker] = worker_counter
                  worker_counter += 1
            else:  # Store the indices associated with each column.
               self.columnXid = {}
               col_num = 0
               for column in row:
                  self.columnXid[column] = col_num
                  col_num += 1
            line_counter += 1
         infile.close()

   # Make a copy of each file that substitutes a worker's real ID with their
   # anonymized ID.
   def make_anonymous_version(self, filenames):
      # Loop through each file.
      for filename in filenames:
         line_counter = 0
         infile = open(filename)
         reader = csv.reader(infile, quotechar='"')
         outfile = open("anonymized_" + filename, "w")
         writer = csv.writer(outfile, quotechar='"')
         for row in reader:
            if line_counter > 0:
               # Anonymize the worker ID.
               worker = row[self.columnXid["WorkerId"]]
               if worker != "WorkerId":
                  row[self.columnXid["WorkerId"]] = self.worker_ids[worker]
            else:  # Store the indices associated with each column.
               self.columnXid = {}
               col_num = 0
               for column in row:
                  self.columnXid[column] = col_num
                  col_num += 1
            line_counter += 1
            writer.writerow(row)
         infile.close()
         outfile.close()

   def Main(self):
      filenames = ["combined_hits_1-100.csv", "new_hits_91-100.csv"]
      self.read_data(filenames)
      self.make_anonymous_version(filenames)

if __name__ == "__main__":
   anonymize_worker_ids = AnonymizeWorkerIDs()
   anonymize_worker_ids.Main()
