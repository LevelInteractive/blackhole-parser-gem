module BlackholeParser
	module Adapters
		class Sql
			@model = nil;

			def create_connection(host, username, password, db_name, adapter)
			    # Variables
			    ipaddr = IPAddr.new host

			    port_hash = {
			          "sqlserver" => 1433,
			          "mysql" => 3306,
			          "mysql2" => 3306,
			          "postgresql" => 5432
			    }

			    begin
			        client = TinyTds::Client.new(:username => username, :password => password, :host => host, :database => db_name, :timeout => 30) if ( ipaddr.ipv4? || ipaddr.ipv6? )
			    rescue
			        raise ConnectionError, "The IP address supplied is invalid.  Please give a valid IPv4 or IPv6 address."
			    end

			    # Establish the connection through ActiveRecord
			    ActiveRecord::Base.establish_connection(
			        :adapter => adapter.to_s,
			        :host => host,
			        :username => username,
			        :password => password,
			        :database => db_name,
			        :port => port_hash[adapter.to_s]
			    )

			    return client
			end

			# Method to get all columns from one table
			def get_columns_from_table(table_name)
				puts "#{table_name} being processed."
				columns = []
				ActiveRecord::Base.connection.columns(table_name).each { |c| columns.push(c.name) }
				return columns
			end


			# Class method to execute a raw SQL statement
			def execute_sql(sql)
				ActiveRecord::Base.connection.execute(sql)
			end

			# Another class method to insert a SQL statement
			def get_last_inserted_id(table_name)
				execute_sql("SELECT * FROM @OutputTbl")
			end

			# Helper method to execute a stored procedure
			def execute_stored_proc(proc_name, *params)
				proc_str = "CALL " << proc_name.to_s << "("
				params.each { |param| proc_str << param.to_s << ", " }
				proc_str = proc_str[0..-3] << ")"  # Remove the end space and comma, then add closing paren

				ActiveRecord::Base.connection.execute(proc_str)
			end

			###############################################
			# Methods specific to ActiveRecord

			# Setter method for model
			def set_model(inc_model)
				@model = inc_model
			end
		end
	end
end