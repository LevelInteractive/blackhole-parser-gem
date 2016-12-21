# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'blackhole_parser/version'

Gem::Specification.new do |spec|
  spec.name          = "blackhole_parser"
  spec.version       = BlackholeParser::VERSION
  spec.authors       = ["Nikhil Venkatesh"]
  spec.email         = ["nikhil.venkatesh08@gmail.com"]
  spec.summary       = %q{A parser that handles parsing CSV, Excel, and JSON files and upserting them to a table in a database.}
  spec.description   = %q{This parser uses local files or files from an Amazon S3 server to map columns from the file to a table specified by the user and then inserts or updates the rows in the table.
                          The Blackhole Parser makes use of common column naming conventions to automatically create the mappings.  The parser allows support of columns to ignore from the input files,
                          custom column mappings between the files and tables, and custom mapping types to ensure there are matches.}
  spec.homepage      = ""
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0")
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.6"
  spec.add_development_dependency "rake"
  spec.add_runtime_dependency "tiny_tds"
  spec.add_runtime_dependency "activerecord-sqlserver-adapter", "~> 4.0.0"
  spec.add_runtime_dependency "activerecord", "4.0.0"
  spec.add_runtime_dependency "composite_primary_keys"
  spec.add_runtime_dependency "aws-sdk", "~> 1"
  spec.add_runtime_dependency "multi_json"
  spec.add_runtime_dependency "roo"
  spec.add_runtime_dependency "ruby-progressbar"
  spec.add_runtime_dependency "parallel"

end
