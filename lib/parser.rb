require 'tiny_tds'
require 'activerecord-sqlserver-adapter'
require 'multi_json'
require 'ruby-progressbar'
require 'blackhole_parser/errors'
require 'blackhole_parser/utils'

module BlackholeParser
	class Parser
		attr_accessor :tables, :files, :column_mapping
		include BlackholeParser::Utils

		DEFAULTS = {
			:datastore => {
				:type => "sqlserver",
				:database => "",
				:table => ""
			},
			:column_mapping => {},
			:ignore_columns => [],
			:mode => "individual",
			:mapping_types => ["automatic"]
		}

		def initialize(options = {})

			# First we check to see if the user specified a configuration file in the options
			# If not, simply set it to whatever they passed in
			@configuration = options.has_key?(:config_file) ? BlackholeParser::Utils.parse_config_file(options[:config_file]).merge(options) : options
			@configuration = DEFAULTS.merge(@configuration)

			puts "#{@configuration}"

			# Store the most used values as variables for easier referral below
			@database = @configuration[:datastore][:database]
			@mode = @configuration[:mode]
			@table_name = @configuration[:datastore][:table]
			# @files = @configuration["files"]
			@upsert_fields = @configuration[:upsert_fields]
			@column_mapping = @configuration[:column_mapping]
			@ignore_columns = @configuration[:ignore_columns]
			@mapping_types = @configuration[:mapping_types]
			@move_to_location = @configuration.has_key?(:s3) ? @configuration[:s3][:move_to] : ""

			# Configure the S3 connection if files isn't specified since files has a higher priority than the S3 connection
			@s3 = configure_s3_server(@configuration[:s3][:access_key_id], @configuration[:s3][:secret_access_key]) if @configuration.has_key?(:s3)

			# Store the connection
			begin
				puts "#{@configuration[:datastore][:host]}, #{@configuration[:datastore][:username]}, #{@configuration[:datastore][:password]}, #{@configuration[:datastore][:database]}"
				@sql_connect = BlackholeParser::Adapters::Sql.new
				@tiny_tds_connect = @sql_connect.create_connection(
					@configuration[:datastore][:host],
					@configuration[:datastore][:username],
					@configuration[:datastore][:password],
					@configuration[:datastore][:database],
					@configuration[:datastore][:type]
				)

			rescue
				raise ConnectionError, "Please enter a database to connect to." if @database.length <= 0
				raise ConnectionError, "The connection to specified database has failed."
			end

			@klass = create_activerecord_class(@table_name)

			# Set the dynamic model we just created in the adapter class
			@sql_connect.set_model(@klass)

		end


		# Method to parse the incoming CSV file
		# Options is a hash that has the key of 'return_values' and an array of columns to return back to the user
		def parse (args = nil, options = {})
			if (@tiny_tds_connect.active?)
				@columns_from_client = []	# previously known as columns_from_csv
				@columns_from_table = []
				@complete_table_hash = {}

				@return_values = options.has_key?(:return_values) ? options[:return_values] : []
				@return_values.map! { |val| val.to_s } if @return_values.length > 0

				puts "Return values: #{@return_values}"

				# puts 'Connection created - ' << @tiny_tds_connect.active?.to_s
				# puts "Using database #{@database}."

				# The arguments can be one of four things
				# 1. A String (either a JSON object or a filename)
				# 2. An array of Hashes. (actual Ruby hashes)
				# 3. An array of Strings (filenames or JSON objects)
				# 4. An array of File objects.

				if args == nil
					# Todo: Create a faster way to go through all the tables - parallel computation?
					# Check if tables is an array of tables?  Not usually the case but have it here anyway.
					if @table_name.kind_of?(Array)
						for index in 0...@table_name.length

							table_name = @table_name[index]
							# Cache the current set of file names (or single file name)
							current_file_set = @files != nil ? @files[index] : nil

							# Process the files from S3 if the user hasn't specified any files otherwise process those
							current_file_set == nil ? process_s3_files(table_name) : process_files(current_file_set, table_name)

						end
					elsif @table_name.kind_of?(String)	# More used case is specifying a single table mapping to one or multiple CSV files

						table_name = @table_name
						# Cache the current set of file names (or single file name)
						file_name = @files != nil ? @files[0] : nil

						file_name == nil ? process_s3_files(table_name) : process_files(file_name, table_name)

					else
						raise ParserError, "You did not enter a valid table name."
					end
				else
					determine_args_type(args, @table_name)
				end

				return @query_returned_values
			end
		end


		# Method that simply goes through the directory and cleans up any locally generated files from S3
		def clean
			# Only look in the top-level of directory
			Dir.entries(".").each do |file|
				if /.xls$/ =~ file || /.csv$/ =~ file 	# Only look for XLS and CSV files
					begin
						File.delete(file)
					rescue
						raise ParserError, "There was an issue deleting #{file}." 	# Stop execution if there is any issues
					end
				end
			end
		end



		private

			# Method that creates the Model class from the ActiveRecord connection
			def create_activerecord_class(table_name)
				primary_keys = []

				# Loop through the columns to get the primary key - we won't know this
				# and although we could have the user input this data, it might be more
				# helpful to just retrieve it ourselves.
				ActiveRecord::Base.connection.columns(table_name).each do |col_obj|
					case @configuration[:datastore][:type]
					when 'sqlserver'
						primary_keys.push(col_obj.instance_variable_get("@name")) if col_obj.instance_variable_get("@sqlserver_options")[:is_primary] == true
					end
				end

				puts "There is/are #{primary_keys.length} primary key(s)."
				# puts "#{primary_keys[0].to_sym}" if primary_keys.length == 1
				# abort("#{primary_keys.map! { |val| val.to_sym }.join(", ")}")

				# Now return the class with the table name and primary keys set
				Class.new(ActiveRecord::Base) do
					self.table_name = table_name

					if primary_keys.length == 1
						self.primary_key = primary_keys[0]	# Set the primary key if there is only 1
					elsif primary_keys.length > 1
						# Otherwise use the composite_primary_keys gem to set the primary keys
						# It uses symbols joined with a comma as the parameters
						self.primary_keys = primary_keys.map! { |val| val.to_sym }.join(", ")
					end
				end

				# Create a public class write_attr to access the private write_attribute method
				# klass.send :define_method, :write_attr do |attr_name = nil, attr_value = nil|
				# 	# In order to call the private method, we have to use .send which accesses ALL of the methods in the class
				# 	self.send :write_attribute, attr_name.to_sym, attr_value
				# end

				# klass
			end




			# Method that sets the columns from the CSV or JSON file
			def set_columns_from_file(file_name, table_name)
				# Reset the instance variable each time we use a new file
				@columns_from_client = []
				parsed_csv = nil
				converted_json = nil

				if file_name.kind_of?(File)	# passing in a File object
					###### TODO need to handle File objects
				elsif file_name.kind_of?(String)
					if /\.json$/ =~ file_name	# Reading a JSON file
						puts "Storing columns from a JSON file..."
						begin
							parsed_json = parse_json(file_name)

							@columns_from_client = parsed_json[0].keys

							converted_json = convert_json_to_csv(parsed_json)

						rescue
							raise ParserError, "Could not read the JSON file."
						end
					elsif /\.csv$/ =~ file_name		# Reading a CSV file
						puts "Storing columns from a CSV file..."
						parsed_csv = parse_csv(file_name)
						@columns_from_client.push(parsed_csv[0])	# Add only the column names from the CSV file
					elsif /\.xls$/ =~ file_name		# Reading an Excel file
						puts "Storing columns from an Excel file..."
						parsed_csv = convert_xls_to_csv(file_name)
						@columns_from_client.push(parsed_csv[0])
					elsif /\.aspx$/ =~ file_name	# Reading an ASPX file
						puts "Storing columns from an ASPX file..."
						parsed_csv = convert_aspx_to_csv(file_name)
						@columns_from_client.push(parsed_csv[0])
					else
						raise ParserError, "Incompatible file.  The parser only accepts .csv, .json, and .xls files."
					end

					parsed_data = (parsed_csv == nil) ? converted_json : parsed_csv
					validate_and_create_query(parsed_data, table_name)
				else
					raise ParserError, "Invalid file type."
				end
			end



			# Method that handles validating the CSV columns and the table columns match
			# and then either raises an error if the columns don't match or creates the upsert query
			def validate_and_create_query(parsed_data, table_name)
				puts "#{@columns_from_client}"

				# Now we flatten the column_names array to have a comprehensive list of column names and make one complete array
				@columns_from_client = @columns_from_client.flatten
				@columns_from_client.uniq!

				# Remove any columns from the CSV that are set to be ignored
				ignore_columns_from_file if @ignore_columns.length > 0

				results = process_table(table_name)
				if results.length > 0
					puts "\nColumns that were in CSV that didn't match with ones in table #{table_name}:"
					puts results

					raise FaultyColumnError, "You have columns in your CSV that didn't match with ones in the table.  If these columns exist in the table, add these columns to the column_mapping hash to handle them."
				else
					# Hack - pass in the SQL connection to use it's escape string abilities
					# @returned_insert_id = BlackholeParser::Utils.create_table_insert_query(table_name, parsed_data, @tiny_tds_connect, @sql_connect, @return_values)
					@query_returned_values = create_insert(@columns_from_client, parsed_data[1...parsed_data.length], @klass, @upsert_fields, @return_values, @new_ignored_columns)
					# create_threaded_insert(@columns_from_client, parsed_data[1...parsed_data.length], @klass, @upsert_fields, @return_values)
				end
				puts "\n"
			end



			# Method to find the column counts between the tables
			# Ideally, each table has unique column names so each column should have a count of 1
			# Tables with the same column name potentially have a foreign key to primary key relationship
			# Important: This method is used when user specifies a full scan, not an individual pairing between CSV files and tables
			def get_column_counts(unique_columns, hash_complete_columns)
				unique_hash = {}
				# Initialize the column counts to 0 for each of the unique columns
				unique_columns.each { |column| unique_hash[column] = 0 }

				# Loop through the full list of columns, incrementing the count when the column name is encountered
				hash_complete_columns.each do |key, value|
					value.each do |column|
						unique_hash[column] += 1 if unique_hash.has_key?(column)
					end
				end

				# puts unique_hash
				return unique_hash
			end



			# Method that handles removing the columns to be ignored from the CSV file
			# The user enters regular expressions without the leading and trailing forward slashes
			# This method loops through each of the regex patterns, stores the indices of the occurrences
			# in the CSV columns array, and then deletes the columns so they aren't taken into account.
			def ignore_columns_from_file
				temp_ignored_columns = []	# Array to hold the indices of the ignored columns in the CSV
				@ignore_columns.each do |column|
					column.strip!
					# Create a new regular expression from the user entered column
					# Case insensitive by default
					column_regex = Regexp.new(column)

					# Store the indices of the columns from the CSV that match the columns to be ignored
					temp_ignored_columns.push(@columns_from_client.each_index.select{ |index| column_regex =~ @columns_from_client[index] })
				end

				# Delete the columns AFTER storing the indices
				@ignore_columns.each { |column| @columns_from_client.delete_if { |csvcolumn| Regexp.new(column) =~ csvcolumn } }

				# Flatten all of the arrays to create one large array of indices
				temp_ignored_columns.flatten!

				#puts temp_ignored_columns

				# Set the ignored columns in the BlackholeParser::Utils
				# This will be set for each file so not to worry
				@new_ignored_columns = BlackholeParser::Utils.set_ignored_columns(temp_ignored_columns)
			end



			# Method that handles looking at the arguments passed into parse
			# and diverting the program to the appropriate path
			def determine_args_type(args, table_name)
				# For switching based on class, we just pass in the args for the case statement
				case args
				when Hash then process_hash(args, table_name)	# User entered a Ruby hash object
				when Array 	# User entered an array...
					if args[0].class == String 	# ...of either JSON objects or filenames
						process_array_of_strings(args, @table_name)
					elsif args[0].class == Hash 	# ...of Ruby Hash objects
						process_array_of_hashes(args, @table_name)
					elsif args[0].class == File 	# ...of Ruby File objects
						process_array_of_files(args, @table_name)
					end
				when String
					puts "Processing a single filename or a JSON string."
					process_array_of_strings([args], @table_name)	# User entered a JSON string or a filename
				end
			end



			# Method that processes a Ruby hash object passed into the parse method
			def process_hash(hash_obj, table_name)
				@columns_from_client = []

				# Set the columns based on the keys in the hash
				hash_obj.keys.each { |key| @columns_from_client.push(key.to_s) }


				# Convert the JSON to CSV to maintain uniformity
				parsed_data = convert_hash_to_csv(hash_obj)

				# Validate the columns and then create the insert query for the row
				validate_and_create_query(parsed_data, table_name)
			end



			# Method to handle an array of strings
			# Each value can be one of two possibilities:
			#   1. A JSON string
			#   2. A filename
			def process_array_of_strings(string_array, table_name)
				@columns_from_client = []

				# Loop through the string array
				# Allow for maximum flexibility - user can input filenames AND JSON strings in one array
				string_array.each_with_index do |str, index|
					# Use a rescue from parsing invalid JSON to check if string is JSON or not
					begin
						##### TODO: Collect all of the JSON strings into one insert query rather than do each one separately
						parsed_json = MultiJson.load(str)

						puts "Processing a JSON string."
						# Now set the columns from the keys in one of the hashes in the array
						@columns_from_client = parsed_json.keys if index == 0	# Only set the columns the first time around

						# Convert the JSON to CSV to maintain uniformity
						parsed_data = convert_json_to_csv(parsed_json)

						# Validate the columns and then create the insert query
						validate_and_create_query(parsed_data, table_name)
					rescue MultiJson::ParseError => exception
						# Not a JSON object, try using a filename instead
						# Raise an error if the filename is not a CSV or JSON file
						raise ParserError, "You did not input a valid value in the array.  The array can only handle JSON strings and filenames." if !(/\.json/ =~ str || /\.csv/ =~ str || /\.xls/ =~ str)

						puts "Processing a single file."
						set_columns_from_file(str, table_name)
					end
				end
			end



			# Method to handle processing arrays of hash objects - common case for Facebook API
			def process_array_of_hashes(hash_array, table_name)
				# Now set the columns from the keys in one of the hashes in the array
				# @columns_from_client = hash_array[0].keys
				hash_array[0].keys.each { |key| @columns_from_client.push(key.to_s) }

				# Convert the JSON to CSV to maintain uniformity
				parsed_data = convert_hash_array_to_csv(hash_array)

				# Validate the columns and then create the insert query
				validate_and_create_query(parsed_data, table_name)
			end



			# Method to handle an array of File objects
			def process_array_of_files(file_array, table_name)
				file_array.each { |file| set_columns_from_file(file, table_name) }
			end



			# Method that processes the files from AWS S3
			def process_s3_files(table_name)
				puts "Processing files from the S3 server."
				ub_devshop = @s3.buckets[@configuration[:s3][:bucket]]

				ub_devshop.objects.with_prefix(@configuration[:s3][:object_prefix]).each do |krobj|
					# if /\.csv$/ =~ krobj.key && /#{table_name}/ =~ krobj.key 	# Assumes that the folders are named a certain way
					if /\.csv$/ =~ krobj.key || /\.xls$/ =~ krobj.key
						file_key = krobj.key.split(/\//)
						file_key = file_key[file_key.length-1]
						File.open(file_key, "wb") do |fp|
							start = Time.now
							progressbar = ProgressBar.create(:title => "Downloading #{file_key}", :starting_at => 0, :total => nil)
							krobj.read do |chunk|
								progressbar.increment
								fp.write(chunk)
							end
							finish = Time.now
							# progressbar.finish
							puts "#{file_key} written. Elapsed time: #{finish - start}"
						end
						puts "Processing file '#{file_key}' for #{table_name}."
						# Check to see if there are any columns amiss and then create the insert query
						set_columns_from_file(file_key, table_name)

						# Move the file in S3 to the completed area
						# Need to append the filename at the end of the move_to directory
						if @move_to_location != ""
							new_location = @move_to_location + file_key
							puts "Moving to #{new_location}"
							moved_krobj = krobj.move_to(new_location)
						end

						# Delete the file after it is processed because we don't need a local copy anymore
						begin
							File.delete(file_key) if File.exist?(file_key)
						rescue
							next
						end

						# Delete CSV files that were generated from XLS to CSV conversion
						csv_key = /.xls$/ =~ file_key ? file_key.chomp('xls') + "csv" : ""
						File.delete(csv_key) if File.exist?(csv_key)

					end
				end # end looping through bucket
			end




			# Method that processes the files
			def process_files(files, table_name)
				if files.kind_of?(String) || files.kind_of?(Array)		# Otherwise files were specified by the user either as a String or Array
					if files.kind_of?(Array)	# The user has multiple files for the table
						files.each do |file|
							puts "Processing file '#{file}' for #{table_name}."
							set_columns_from_file(file, table_name)
						end
					elsif files.kind_of?(String)	# The user specified a single file for the table
						puts "Processing file '#{files}' for #{table_name}."
						set_columns_from_file(files, table_name)
					end
				else
					raise ParserError, "The files you specified weren't in an appropriate format.  You can enter arrays of files or single file names."
				end
			end



			# Method that handles the actual parsing from the tables and maps the columns
			def process_table(table_name)
				@columns_from_table = []
				@complete_table_hash = {}

				# Retrieve the columns from the table we are mapping to
				parsed_cols = @sql_connect.get_columns_from_table(table_name)
				@columns_from_table.push(parsed_cols)
				@complete_table_hash["#{table_name}"] = parsed_cols	# Add the columns for each table in the hash using the table name as the key

				# Store all of the unique columns between the tables
				cols_from_table_unique = @columns_from_table.flatten.uniq

				# Calculate the counts of the columns between the tables
				unique_hash = get_column_counts(cols_from_table_unique, @complete_table_hash)

				# Final step is to map out the columns from the CSV files to the table and retrieve any columns that don't match both the CSV and table
				# Params: The full list of columns from the CSV files, the hash of the unique columns between the tables and their 'uniqueness counts', and the user defined column mapping
				error_columns = BlackholeParser::Utils.map_columns(@columns_from_client, unique_hash, @column_mapping, @mapping_types)

				return error_columns
			end
	end
end