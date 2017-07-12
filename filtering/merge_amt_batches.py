###############################################################################
# merge_amt_batches.py
#
# Natalie Parde
# 4/12/2017
#
# Merges an existing AMT results file (with approvals and rejections already
# indicated) with an AMT results file for a new batch of HITs.
#
###############################################################################

import csv
import operator
from time import gmtime, strftime

class AMTBatchMerger:

   # Read in the current combined file, and the new file to be merged with it.
   # Store the rows associated with each, and the column names and indices
   # associated with each column name.
   def read_files(self, combined_filename, new_filename):
      # Read in the combined file and store indices for all the columns that it
      # currently contains.
      infile = open(combined_filename)
      reader = csv.reader(infile, quotechar='"')
      
      row_num = 0
      self.combined_file_rows = []
      for row in reader:
         if row_num > 0:
            self.combined_file_rows.append(row)
         else:  # Store the indices associated with each column; makes indexing more transparent in the future.
            self.columnXid = {}
            col_num = 0
            for column in row:
               self.columnXid[column] = col_num
               col_num += 1
         row_num += 1
      infile.close()

      # Read in the new file and store indices for all the columns that it
      # currently contains.
      infile = open(new_filename)
      reader = csv.reader(infile, quotechar='"')
      
      row_num = 0
      self.new_file_rows = []
      for row in reader:
         if row_num > 0:
            self.new_file_rows.append(row)
         else:  # Store the indices associated with each column; makes indexing more transparent in the future.
            self.new_columnXid = {}
            col_num = 0
            for column in row:
               self.new_columnXid[column] = col_num
               col_num += 1
         row_num += 1
      infile.close()

   # Compare the column names of the current combined file and the new file with
   # one another.  In cases where the combined file has a column that the new file
   # does not, add that column to the data from the new file in the correct
   # location (with blank cells).  In cases where the new file has a column that
   # the combined file does not, add that column to the data from the combined
   # file in the correct location (with blank cells).
   def update_columns(self):
      # First, check the new data file for columns in the combined file that it
      # does not have.
      sorted_column_names = sorted(self.columnXid.items(), key=operator.itemgetter(1))
      last_column_found = ""
      offset = 0  # As more columns are added to the new file, we'll have to offset the indices stored in self.new_columnXid.
      for column_name, column_idx in sorted_column_names:
         if column_name not in self.new_columnXid:
            print "The new file does not have a column named: " + column_name
            print "It should be added after the last column found, which was: " + last_column_found

            # Get the index of last_column_found in the new file.
            i_last_column_found = self.new_columnXid[last_column_found]
            for row in self.new_file_rows:
               # Add a blank cell in i_last_column_found+1 for each row of data
               # in the new file.
               row.insert(i_last_column_found+1, "")
            last_column_found = column_name
            self.new_columnXid[column_name] = i_last_column_found + 1
            offset += 1
         else:
            last_column_found = column_name
            self.new_columnXid[column_name] += offset

      # Next, check the combined data file for columns in the new file that it does
      # not have.
      sorted_column_names = sorted(self.new_columnXid.items(), key=operator.itemgetter(1))
      last_column_found = ""
      offset = 0  # As more columns are added to the new file, we'll have to offset the indices stored in self.new_columnXid.
      for column_name, column_idx in sorted_column_names:
         if column_name not in self.columnXid:
            print "The combined file does not have a column named: " + column_name
            print "It should be added after the last column found, which was: " + last_column_found

            # Get the index of last_column_found in the combined file.
            i_last_column_found = self.columnXid[last_column_found]
            for row in self.combined_file_rows:
               # Add a blank cell in i_last_column_found+1 for each row of data
               # in the combined file.
               row.insert(i_last_column_found+1, "")
            last_column_found = column_name
            self.columnXid[column_name] = i_last_column_found + 1
            offset += 1
         else:
            last_column_found = column_name
            self.columnXid[column_name] += offset

   # Merge the two files by first printing the updated combined file data lines,
   # and then printing the updated new file data lines.  Keep header lines so it's
   # easy to determine where different batches start and end.
   def merge_files(self, output_dir):
      outfile = open(output_dir + "/" + strftime("%Y-%m-%d_%H:%M:%S", gmtime()) + "_combined_hits.csv", "w")
      writer = csv.writer(outfile, quotechar='"')

      # Get the updated column names dictionary, sorted by index.
      sorted_column_indices = sorted(self.new_columnXid.items(), key=operator.itemgetter(1))
      header = []
      for column_name, column_idx in sorted_column_indices:
         header.append(column_name)
      writer.writerow(header)

      # Write out the updated combined file rows.
      for row in self.combined_file_rows:
         if row[0] == "HITId":
            writer.writerow(header)
         else:
            writer.writerow(row)
      
      # Write out the updated new file rows.
      writer.writerow(header)
      for row in self.new_file_rows:
         if row[0] == "HITId":
            writer.writerow(header)
         else:
            writer.writerow(row)
      try:
         outfile.close()
      except IOError as e:
         print e

if __name__ == '__main__':
   amt_batch_merger = AMTBatchMerger()
   input_dir = "sample_input"
   output_dir = "sample_output"
   existing_hit_file = "anonymized_combined_hits_1-90.csv"
   new_hit_file = "anonymized_new_hits_91-100.csv"
   amt_batch_merger.read_files(input_dir + "/" + existing_hit_file, input_dir + "/" + new_hit_file)
   amt_batch_merger.update_columns()
   amt_batch_merger.merge_files(output_dir)
