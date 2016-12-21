module BlackholeParser
	#############################################
	# Custom exception classes
	#############################################
	class FaultyColumnError < RuntimeError ; end
	class ConnectionError < RuntimeError ; end
	class ParserError < RuntimeError ; end
end