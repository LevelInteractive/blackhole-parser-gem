module BlackholeParser
	module Utils
		@@mappings_hash = {}
		@actual_csv_order = []
		@ignored_columns = []



		# Method to create the S3 connection
		def configure_s3_server(access_key_id, secret_access_key)
			puts "Connected to AWS S3 server."
			return AWS::S3.new(
			  :access_key_id => access_key_id,
			  :secret_access_key => secret_access_key
			)
		end


		# Method to set environment variables
		def self.set_env_var(key, value)
			ENV[key.to_s] = value
		end


		# Method to parse the config YAML file and return a hash
		def self.parse_config_file(file_name)
			config_file = YAML.load(File.open(file_name))
			return config_file
		end


		# Method to parse CSV file
		# Apparently we need to handle different types of encodings...which is fantastic
		def parse_csv(csv_file)
			# Fixed errors due to a Byte-Order-Mark (BOM) at the very beginning of some CSV files
			# http://stackoverflow.com/questions/23011713/illegal-quoting-error-with-ruby-csv-parsing
			parsed_csv = []
			begin
		  		parsed_csv = CSV.read(csv_file, :encoding => 'bom|utf-8')
			rescue ArgumentError
		  		begin
		    		parsed_csv = CSV.read(csv_file, :encoding => 'bom|utf-8:ISO-8859-1')
		  		rescue ArgumentError
		    		raise ParserError, "There was an error in reading the CSV encoding."
				end
			end
			return parsed_csv
		end


		# Method to write to a common CSV file from multiple ones
		def self.write_to_csv(csv_file, csv_files)
			puts csv_files.length
			CSV.open(csv_file, "a+") do |csv|
				for i in 0...csv_files.length
		    		p_csv = parse_csv(csv_files[i])
		    		p_csv.each_with_index do |row, index|
		      			# Don't write the column headers for subsequent CSV files after the first one
		    			next if i > 0 && index == 0
		    			csv << row
		    		end
				end
			end
		end


		# Method to parse JSON file
		def parse_json(json_file)
			jfile = File.read(json_file)
			return JSON.parse(jfile)
		end


		# Method to convert JSON to CSV to maintain current code structure
		def convert_json_to_csv(parsed_json)
			raise ParserError, "The JSON to parse is empty." if parsed_json.length <= 0

			csv_string = CSV.generate do |csv|
				csv << parsed_json[0].keys
		  		parsed_json.each do |hash|
		  			csv << hash.values
		  		end
			end
			return CSV.parse(csv_string, :encoding => 'bom|utf-8')
		end


		# Method to convert a single hash to CSV
		def convert_hash_to_csv(hash_obj)
			temp_columns = []
			hash_obj.keys.each { |key| temp_columns.push(key.to_s) }

			csv_string = CSV.generate do |csv|
				csv << temp_columns		# Add the column names
				csv << hash_obj.values
			end
			return CSV.parse(csv_string, :encoding => 'bom|utf-8')
		end


		# Method to convert an array of hashes to CSV
		def convert_hash_array_to_csv(hash_array)
			csv_string = CSV.generate do |csv|
				csv << hash_array.first.keys # adds the attributes name on the first line
		    	hash_array.each do |hash|
		    		csv << hash.values
		    	end
			end
			return CSV.parse(csv_string, :encoding => 'bom|utf-8')
		end


		# Method to convert an XLS file to CSV
		def convert_xls_to_csv(xls_file)
			raise ParserError, "The file is not in the correct format." if !(/.xls$/ =~ xls_file)
			pwd = File.expand_path('./files')
			xls = nil
			csv_filename = ""

			file_basename = xls_file.split(/\//)
			file_basename = file_basename[file_basename.length - 1]


			begin
			  xls_file_path = "#{pwd}/#{file_basename}"
			  p xls_file_path
			  xls = Roo::Excel.new(xls_file_path)
			  csv_filename = "#{pwd}/#{file_basename}".chomp!('xls') + "csv"
			rescue
				p file_basename
			  xls = Roo::Excel.new(file_basename)
			  csv_filename = "#{file_basename}".chomp!('xls') + "csv"
			end

			xls.to_csv(csv_filename)  # use the Roo gem's to_csv method to convert the XLS file to CSV

			parsed_csv = parse_csv(csv_filename)  # Return the CSV file as an array of arrays for normal processing
			File.delete(csv_filename) # Delete the file after parsing and storing the rows
			return parsed_csv
		end


		# Method that converts .aspx files to CSV
	  	def convert_aspx_to_csv(aspx_file)
	  		raise ParserError, "The file is not in the correct format." if !(/.aspx$/ =~ aspx_file)
	  		file_name = File.basename(aspx_file).chomp("aspx")		# Store just the filename without any extensions

	  		absolute_path = File.absolute_path(aspx_file)	# Get the absolute path sto the file - from S3 it could be stored in current working directory, from local, it could be anywhere

	  		path_to_file = absolute_path.split(/\//)

	  		path_to_file.delete_at(path_to_file.length - 1)	# Delete the filename at the end and only retain the directories leading to it

	  		path_to_file = path_to_file.join("/")	# Join the directories with forward slashes

	  		# src = File.open(aspx_file)
	  		dest = File.open("#{path_to_file}/#{file_name}html", 'wb')
	  		# IO.copy_stream(src, dest)	# Copy the contents of the file over

	  		File.foreach(aspx_file).with_index do |line, line_num|
	  			puts "Line #{line_num}"
	  			line_to_write = line.split(/>(?=<)/)
	  			line_to_write = line_to_write.join(">\n")
	  			dest.write(line_to_write)
	  		end
	  		abort("Finished writing")
	  	end


		# Method that maps the columns between the CSV files and the specified table
		# Check the custom column mapping to see if user specified if a column from the CSV should be mapped to multiple tables
		def self.map_columns(csv_cols, table_cols_hash, custom_mappings_hash, mapping_types)
			# Variable to hold the fields that completely don't match in the table
			@@mappings_hash = {}  # Stores the hash of CSV columns (original before any modifications) with the value as the array of different mappings
			@actual_csv_order = []  # Stores the order of the CSV columns but with the appropriate mappings from the table
			error_columns = []  # Stores any columns that are in the CSV that aren't in the table

			@mapping_types = mapping_types if mapping_types.length > 0  # Store the user entered mapping types (automatic, underscores, lowercase, etc.)

			# Convert the custom mappings all to lowercase
			# custom_mappings_hash = convert_custom_mappings(custom_mappings_hash)
			puts custom_mappings_hash

			# First convert everything to lowercase
			csv_cols.each do |column|
			  	# Store the original column name
			  	orig_column = column

			  	# Before doing anything, there might be some CamelCased columns in the CSV that need to be formatted correctly
			  	# Don't do anything if the user has specified a custom mapping!
			  	column = check_camel_case(column, custom_mappings_hash)

			  	# Convert the column to lowercase
			  	lc_column = column.to_s.downcase

			  	# Check if column has any numbers and convert it
			  	if column =~ /\d/
			    	column = convert_num_to_string(column)
			  	end

			  	@@mappings_hash[orig_column] = custom_mappings_hash.has_key?(orig_column) ? create_mappings(column, custom_mappings_hash[orig_column]) : create_mappings(column)
			end

			# Now we have to check the table_cols_hash agains the mappings
			# Loop through the hash
			@@mappings_hash.each do |key, value|
			  success_counter = 0
			  failure_counter = 0
			  value.each do |mapping|
			    if table_cols_hash.has_key?(mapping)
			      @actual_csv_order.push(mapping) if !@actual_csv_order.include?(mapping)
			      success_counter += 1
			      # puts "================== #{key} is in both csv and table.  It's value is #{mapping}. ====================" unless success_counter > 1
			    else
			      failure_counter += 1
			      error_columns.push(key) if failure_counter === value.length
			    end

			  end #end looping through each mapping for each column
			end #end looping through all of the columns from the CSV

			return error_columns
		end # end map_columns method


		# Method that I'm testing for parallel file parsing and insertion
		def create_threaded_insert(headers, values, model, upsert_fields, ret_vals)
			num_rows = values.length		# Cache the number of rows we have to insert
			num_threads = (num_rows.to_f / 7000).ceil	# Calculate the number of threads using an arbitrary way (for now...)
			threads = []		# Create an array to hold the threads we are going to spawn to handle the file

			# IMPORTANT: We create a new array using the values array and splitting it with chunks of 7000 rows
			new_values = values.each_slice(7000).to_a

			# abort("#{new_values[0]}")

			p "There are #{num_rows} rows to insert."
			p "We are creating #{num_threads} threads to handle the file."

			#headers.map! { |head_val| head_val.gsub("/\"/", "'");  }

			#abort("#{headers}")

			# Use the Parallel gem to handle parallel computation of the insertions
			results = Parallel.map(new_values, :in_threads=>num_threads) do |val|
				p headers
				create_insert(headers, val, model, upsert_fields, ret_vals)
			end

			# We can try and maximize the threads to attain the best performance.
			# For now, we will just use an arbitrarily calculated number of threads.
			# num_threads.times do |i|
			# 	threads[i] = Thread.new {
			# 		create_insert(headers, new_values[i], model, upsert_fields, ret_vals)
			# 	}
			# end

			# threads.each { |thread| thread.join }
			# ThreadsWait.all_waits(*threads)
			puts "Finished inserting data successfully!"
		end


		# Main method to insert the rows from a table name, an array of header columns
		# and an array of arrays for the values
		def create_insert(headers, values, model, upsert_fields, ret_vals, ig_cols)
			puts "Creating insert query:"
			puts "There are #{values.length} rows to insert."

			p "HEADERS:"
			p headers

			p "IGNORED COLUMNS BEFORE HACK:"
			p ig_cols

			ig_cols = [] if ig_cols == nil	# Weird hack because of an error ruby was throwing
			return_results = []

			p "IGNORED COLUMNS:"
			p ig_cols

			# Loop through the array of arrays of values to insert
			values.each do |values_array|
				upsert_attributes = {}
				inner_array = []
				# Now loop through the single array of values
				p "VALUES ARRAY:"
				p values_array

				values_array.each_with_index do |val, index|
					# puts "INDEX: #{index}"

					next if ig_cols.include?(index)  # IMPORTANT: Need to ignore the indices of the columns in the CSV that the user specifies
					associated_column_name = headers.at(index).to_sym	# Get the header name for the row - need it to match in return values

					# Store the attributes we want to do the upsert on to pass into find_or_create_by method
					upsert_attributes[associated_column_name] = val if upsert_fields.include?(associated_column_name)

					# puts "Line 282: #{upsert_attributes}"
				end

				# Use ActiveRecord's method to return the updated or inserted row
				# Workaround - do a select and then insert since I can't figure out how to dynamically add the values to the class
				# select_result = model.find_by(upsert_attributes)
				insert_attributes = {}
				values_array.each_with_index do |val, i|
					if !(upsert_attributes.has_key?(headers[i]))
						# puts "VALUE: #{val}"
						insert_attributes[headers[i].to_sym] = val
					end
				end
				insert_attributes = insert_attributes.merge upsert_attributes

				# if select_result == nil
				# 	insert_result = model.create(insert_attributes)
				# else
				# 	insert_result = model.update(insert_attributes)
				# end

				# upsert_result = model.find_or_create_by(upsert_attributes) do |klass|
				# 	# Check to see that we haven't already included the column and value in the upsert_attributes
				# 	# and if we haven't, include it as a field we need to add to the database along with the value
				# 	# puts "#{klass.instance_variables}"
				# 	values_array.each_with_index do |val, i|
				# 		if !(upsert_attributes.has_key?(headers[i]))
				# 			# puts "VALUE: #{val}"
				# 			klass.send :write_attribute, headers[i].to_sym, val
				# 		end
				# 	end
				# end

				p upsert_attributes

				upsert_result = model.find_or_initialize_by(upsert_attributes)
				upsert_result.update_attributes(insert_attributes)

				# Return what the user asked for
				#ret_vals.each { |val| inner_array.push(insert_result[val]) }
				ret_vals.each { |val| inner_array.push(upsert_result[val]) }

				# Concatenate the arrays of information the user wants back
				return_results.push(inner_array)

			end
			# p return_results
			return return_results
		end




		# Main method to insert the CSV rows into the specified table
		def self.create_table_insert_query(table_name, parsed_csv, client, sql_connection, ret_vals)

			puts "Creating insert query:"
			puts "CSV file has #{parsed_csv.length} rows."

			table_column_types = []
			row_counter = 0
			ret_id = -1;

			puts "RET VALS FROM USER: #{ret_vals}"

			@return_values = []
			@return_values = ret_vals

			# Now it's time to add in the rows to the tables
			# Create the INSERT string - a multiple value insert is faster
			insert_query = create_insert_prefix(table_name)
			for l in 0...parsed_csv.length  # Change back to parsed_csv.length!!!!
			  row = parsed_csv[l]
			  if l == 0
			    table_column_types = get_column_types(table_name, row)
			    # puts table_column_types
			  else
			    next if row.length == 0     # In case there are empty lines written to the CSV file, we need to ignore these!!
			    tcounter = 0  # table counter to go through the table columns iteratively

			    insert_query << '('

			    for m in 0...row.length
			      next if @ignored_columns.include?(m)  # IMPORTANT: Need to ignore the indices of the columns in the CSV that the user specifies
			      # Preventative measure in case there is a mistake and the program doesn't correctly handle ignoring columns properly
			      abort("Too many row columns compared to table columns. #{@ignored_columns} #{row.length}") if tcounter > table_column_types.length
			      element = (row[m] == nil || row[m].strip! == "") ? "" : row[m]
			      # puts "Current element: #{element} whose type is #{table_column_types[tcounter]}"

			      case table_column_types[tcounter]
			      when "string" then element = "'" << client.escape(element.encode!("UTF-8", :invalid => :replace, :replace => "?")) << "'"
			      when "integer" then element = element.to_i
			      when "decimal" then element = element.to_f
			      when "float" then element = element.to_f
			      when "datetime"
			        tmp_elem = element
			        if /\+\d{2}:\d{2}$/ =~ element  # When parsing Excel files with dates, it adds an '+00:00' after the hours, minutes, and seconds
			          tmp_elem = element.split(/\+/)  # so we have to remove that in order to insert it into SQL Server
			          tmp_elem = tmp_elem[0]
			        end
			        element = "'" << client.escape(tmp_elem) << "'"
			      # when "datetime" then element = "'" << Utilities.convert_to_datetime(element) << "'"
			      end

			      insert_query << element.to_s << ", "
			      tcounter += 1
			    end
			    insert_query = insert_query[0..-3]  # Remove the space and comma
			    insert_query << '), '
			    puts "Created query #{l}"

			    # There is a row insert maximum in SQL Server of 1000 rows...
			    # Another bug is that TinyTDS keeps timing out if you do the max of 1000 rows
			    # so to avoid this, we are just adding 500 at a time
			    if row_counter == 499
			      insert_query = insert_query[0..-3]  # Remove the last space and comma
			      #puts insert_query

			      # Execute the query!
			      insert_query << " SELECT * FROM @OutputTbl"
			      ret_id = sql_connection.execute_sql(insert_query)
			      # ret_id = sql_connection.get_last_inserted_id(table_name)

			      # Reset the insert query by creating the beginning of the statement
			      insert_query = create_insert_prefix(table_name)
			      row_counter = 0
			    end

			    row_counter += 1
			  end # End if statement to skip first row
			end # End for loop for the CSV file

			# If there are any leftover rows, insert them here
			if row_counter > 0
			  	insert_query = insert_query[0..-3]  # Remove the last space and comma
			  	# puts insert_query

			  	# Execute the query!
			  	puts insert_query

			  	sql_connection.execute_sql(insert_query) if !insert_query[-4..-1].eql?("VALU") && !insert_query[-6..-1].eql?("VALUE)")
			  	ret_id = sql_connection.execute_sql("SELECT ")

			  	puts ret_id
		      # ret_id = sql_connection.get_last_inserted_id(table_name)

			end

			puts "Data from the files added successfully!"
			# Reset tcounter?
			tcounter = 0

			return ret_id
		end # End create_table_insert_query method




		# Setter method to retrieve the indexes of the ignored columns in the CSV
		def self.set_ignored_columns(ic)
			# Reset the ignored_columns each time!!
			@ignored_columns = []
			ic.each { |column| @ignored_columns.push(column) }

			p "IGNORED COLUMNS INITIAL"
			p @ignored_columns
			# puts @ignored_columns.kind_of?(Array)
			return @ignored_columns
		end

		private

		# Helper method to convert numbers to string equivalent
		def self.convert_num_to_string(num_str)
		  converted_str = num_str
		  num_mappings = {
		    "1" => "fir",
		    "2" => "seco",
		    "3" => "thi",
		    "4" => "four",
		    "5" => "fif",
		    "6" => "six",
		    "7" => "seven",
		    "8" => "eigh",
		    "9" => "nin",
		    "10" => "ten",
		    "11" => "eleven",
		    "12" => "twelf",
		    "13" => "thirteen",
		    "14" => "fourteen"
		  }

		  # puts num_str
		  converted_str = converted_str.gsub(/(\d)/) { |match| num_mappings[match] } if num_str =~ /\d[a-zA-Z]{2}/

		  return converted_str
		end

		# Helper method to convert all the custom column mapping keys to be lowercase and uniform
		def self.convert_custom_mappings(custom_mappings_hash)
		  ret_hash = {}
		  custom_mappings_hash.keys.each do |key|
		    ret_hash[key.downcase] = custom_mappings_hash[key]
		  end

		  return ret_hash
		end



		# Helper method to check if column is CamelCased so we can handle it appropriately
		def self.check_camel_case(column, custom_mappings_hash)
		  ret_column = column
		  if /^([A-Z][a-z0-9]+){2,}$/.match(column)
		    if !custom_mappings_hash.has_key?(column.downcase)
		      ret_column = convert_camel_case(column)
		      puts "Camel case found: #{column}"
		    end
		  end
		  return ret_column
		end


		# Helper method to convert CamelCased strings to the format we want
		# Basically it will convert camel cased strings to have the words separated by spaces
		def self.convert_camel_case(input_str)
		  ret_str = ""
		  input_str = input_str.split(/(?=[A-Z])/)
		  input_str.each { |spl_str| ret_str << spl_str << " " }
		  return ret_str.strip!
		end



		# Helper method to convert a string to a compatible DateTime object
		def self.convert_to_datetime(input_str)
		  #puts input_str
		  # Check if there is are seconds in the time
		  # input_str_split = input_str.split(" ")
		  # date_part = input_str_split[1].split(/\//)
		  # month = date_part[0]
		  # day = date_part[1]
		  # year = date_part[2]

		  # #puts input_str_split[2]
		  # # If there are seconds, include it in the format otherwise leave it out
		  # parsed_time = input_str_split[2] != nil ? DateTime.strptime(input_str, '%m/%d/%Y %l:%M:%S %p') : DateTime.strptime(input_str, '%m/%d/%Y %l:%M:%S %p')
		  # return parsed_time.to_s
		  return input_str
		end



		# Helper method to create the beginning of a SQL Insert query so we don't have to keep doing it above
		def self.create_insert_prefix(table_name)
			# First declare a temporary table to store the requested information back
			# We always return the last inserted ID
			insert_query = "DECLARE @OutputTbl TABLE ("

			t_ret_val = []   #CHANGE THIS
			@return_values.each { |val| t_ret_val.push(val) }

			@return_values.each { |val| insert_query << val << " " << get_column_type(table_name, val) << ", " } if @return_values.length > 0
			insert_query = insert_query[0..-3]	# Remove the comma and the space
			insert_query << ")"

			# Begin the actual INSERT statement with the column names
		  	insert_query << " INSERT INTO [dbo].[#{table_name}]("
		  	# Add the table fields here wrapped in []
		  	@actual_csv_order.each do |column_name|
		    	insert_query << "[" << column_name << "],"
		  	end
		  	insert_query = insert_query[0...-1]  # Remove the last comma

		  	temp_return_values = @return_values.collect {|item|  "INSERTED." + item }

		  	# The OUTPUT statement that is needed to gather the requested information and store it in the table
		  	insert_query << ") OUTPUT #{temp_return_values.join(", ") } INTO @OutputTbl("

		  	puts "#{t_ret_val}"

		  	t_ret_val.each { |val| insert_query << val << ", " } if @return_values.length > 0
			insert_query = insert_query[0..-3]	# Remove the comma and the space

		  	insert_query << ") VALUES"
		  	return insert_query
		end




		# Helper method to create mappings
		# @return array of variations
		def self.create_mappings(field, custom_mapping = "")
		  ret_array = []

		  # IMPORTANT - We have to explicitly convert the string to ASCII otherwise it won't match the string at all.
		  encoding_options = {
		    :invalid           => :replace,  # Replace invalid byte sequences
		    :undef             => :replace,  # Replace anything not defined in ASCII
		    :replace           => '',        # Use a blank for those replacements
		    :UNIVERSAL_NEWLINE_DECORATOR => true       # Always break lines with \n
		  }
		  field = field.encode(Encoding.find('ASCII'), encoding_options)

		  # puts "ASCII: #{field.ascii_only?}"
		  # Remove leading and trailing whitespace
		  field = field.strip


		  reg_field = field   # Case as itself
		  lc_field = field.downcase  # Case as lowercase
		  uc_field = field.upcase  # Case as uppercase

		  underscore_field = field.gsub(/\s+/, "_")  # Case with regular field and underscores
		  lc_underscore_field = field.downcase.gsub(/\s+/, "_")  # Case with lowercase field and underscores
		  uc_underscore_field = field.upcase.gsub(/\s+/, "_")  # Case with uppercase field and underscores

		  dash_field = field.gsub(/\s+/, "-")  # Case with dashes in table
		  lc_dash_field = field.downcase.gsub(/\s+/, "-")  # Case with lowercase field and dashes
		  uc_dash_field = field.upcase.gsub(/\s+/, "-")  # Case with uppercase field and dashes

		  concat_field = field.gsub(/\s+/, "")  # Case with simple concatenation of words
		  lc_concat_field = field.downcase.gsub(/\s+/, "")  # Case with lowercase field and concatenation
		  uc_concat_field = field.upcase.gsub(/\s+/, "")  # Case with uppercase field and concatenation

		  cap_field = field.capitalize  # Case with concatenation and capitalize first letter

		  @mapping_types.each do |type|
		    case type
		    when "automatic"
		      ret_array.push(reg_field, lc_field, uc_field, underscore_field, lc_underscore_field, uc_underscore_field,
		                      dash_field, lc_dash_field, uc_dash_field, concat_field, lc_concat_field, uc_concat_field, cap_field)
		    when "lowercase"
		      ret_array.push(lc_field, lc_underscore_field, lc_dash_field, lc_concat_field)
		    when "uppercase"
		      ret_array.push(uc_field, uc_underscore_field, uc_dash_field, uc_concat_field)
		    when "underscores"
		      ret_array.push(reg_field, lc_field, uc_field, underscore_field, lc_underscore_field, uc_underscore_field)
		    when "dashes"
		      ret_array.push(reg_field, lc_field, uc_field, dash_field, lc_dash_field, uc_dash_field)
		    when "concatenation"
		      ret_array.push(reg_field, lc_field, uc_field, concat_field, lc_concat_field, uc_concat_field)
		    end
		  end



		  #custom_mapping.keys.each { |key| ret_array.push(key) } if custom_mapping.length > 0
		  # Add the custom mappings passed in for this column name
		  ret_array.push(custom_mapping) if !custom_mapping.strip!.eql?("")

		  # Convert to a merged Set to remove any duplicates
		  s1 = Set.new []
		  s1 = s1.merge(ret_array)

		  return s1.to_a
		end



		# Method that gets the column type for a specified column
		def self.get_column_type(table_name, column_name)
			column_type = ""
			ActiveRecord::Base.connection.columns(table_name).each do |c|
				next if !c.name.eql?(column_name)
				if c.name.eql?(column_name)
					column_type = c.type.to_s
					break
				else
					raise ParserError, "The column specified by the user is not in the table.  Please make sure you have spelled the column correctly."
				end
			end
			case column_type
			when "string" then return "VARCHAR(2048)"
			when "decimal" then return "DECIMAL(10, 4)"
			when "float" then return "FLOAT"
			when "integer" then return "INT"
			when "datetime" then return "DATETIME"
			end
		end


		# Method to get all of the column's types for a table
		def self.get_column_types(table_name, row)
		  column_types = []
		  @@mappings_hash.each do |key, value|
		    success_counter = 0
		    ActiveRecord::Base.connection.columns(table_name).each do |c|
		      value.each do |mapping|
		        next if !c.name.eql?(mapping)
		        if c.name.eql?(mapping)
		          success_counter += 1
		          # puts "Table column name: #{c.name}, Mapping: #{mapping}"
		          column_types.push(c.type.to_s) unless success_counter > 1
		        end
		      end #end looping through each mapping for each column
		    end # end ActiveRecord looping through columns
		  end #end looping through all of the columns from the CSV

		  # puts column_types
		  return column_types
		end
	end
end