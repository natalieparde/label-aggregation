###############################################################################
#
# merge_AMT_results_files.py
#
# Natalie Parde
#
# Merge all AMT results files matching the specified pattern to a single 
# combined file.
#
###############################################################################

import os
import csv
import glob
import operator

class MergeResultsFiles:

   # Get all of the (final) AMT results files associated with this pattern.
   def get_matching_files(self, pattern, input_dir):
      self.filenames = glob.glob(input_dir + "/*" + pattern + "*")
      print self.filenames

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
         else:  # Store the indices associated with each column.
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
      # First, combine all the id-related column names across the two files.
      id_related_columns = []
      for column_name in self.columnXid:
         if column_name.startswith("Answer.") and "example" not in column_name:
            id_related_columns.append(column_name)

      for column_name in self.new_columnXid:
         if column_name.startswith("Answer.") and "example" not in column_name:
            id_related_columns.append(column_name)

      # Remove duplicates.
      id_related_columns = list(set(id_related_columns))

      # Sort the column names.
      sorted_column_names = sorted(id_related_columns)

      # Assign new columnXid values based on the combination.
      idx = self.columnXid["Answer.0metaphorNovelty0"]
      for column_name in sorted_column_names:
         if "metaphorNovelty" in column_name:
            if column_name not in self.new_columnXid:
               print "The new file does not have a column named: " + column_name
               print "It should be added after the last column found, which was: " + last_column_found
               for row in self.new_file_rows:
                  # Add a blank cell in this column for each row of data in the new file.
                  row.insert(idx, "")
            if column_name not in self.columnXid:
               print "The combined file does not have a column named: " + column_name
               print "It should be added after the last column found, which was: " + last_column_found
               for row in self.combined_file_rows:
                  # Add a blank cell in this column for each row of data in the new file.
                  row.insert(idx, "")
            self.columnXid[column_name] = idx
            self.new_columnXid[column_name] = idx
            idx += 1
            last_column_found = column_name
      self.columnXid["Answer.example1"] = idx+1
      self.new_columnXid["Answer.example1"] = idx+1
      self.columnXid["Answer.example2"] = idx+2
      self.new_columnXid["Answer.example2"] = idx+2
      self.columnXid["Answer.example3"] = idx+3
      self.new_columnXid["Answer.example3"] = idx+3
      self.columnXid["Approve"] = idx+4
      self.new_columnXid["Approve"] = idx+4
      self.columnXid["Reject"] = idx+5
      self.new_columnXid["Reject"] = idx+5


   # Merge the two files by first printing the updated combined file data lines,
   # and then printing the updated new file data lines.
   def merge_files(self, pattern, output_dir):
      outfile = open(output_dir + "/" + pattern + "_annotations.csv", "w")
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
      for row in self.new_file_rows:
         if row[0] == "HITId":
            writer.writerow(header)
         else:
            writer.writerow(row)
      outfile.close()

   # Merge all of the files associated with the pattern into a single new file.
   def merge_all(self, pattern, output_dir):
      for x in range(1,len(self.filenames)):
         if x == 1:  # No combined file exists yet; just take the first two files.
            combined_file = self.filenames[0]
            new_file = self.filenames[1]
         else:  # We've already merged the first two files; now we're adding to that combined file.
            combined_file = output_dir + "/" + pattern + "_annotations.csv"
            new_file = self.filenames[x]

         # Combine the specified files.
         print "Combining files", combined_file, "and", new_file, "...."
         self.read_files(combined_file, new_file)
         self.update_columns()
         self.merge_files(pattern, output_dir)


   def Main(self):
      input_dir = "sample_input"
      output_dir = "sample_output"
      pattern = "anonymized_amt"
      self.get_matching_files(pattern, input_dir)
      self.merge_all(pattern, output_dir)

if __name__ == '__main__':
   merge_AMT_results_files = MergeResultsFiles()
   merge_AMT_results_files.Main()
