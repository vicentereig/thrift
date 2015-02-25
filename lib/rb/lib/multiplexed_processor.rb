module Thrift
	class MultiplexedProcessor
		def initialize
			@map = Hash.new
		end

		def register(service, processor)
			processor.instance_variable_set(:@service, service)
			@map[service] = processor
		end

		def process(iprot, oprot)
			name, type, seqid  = iprot.read_message_begin

			service, method = name.split(':')
			processor = @map[service]
			if processor.respond_to?("process_#{method}")
				processor.send("process_#{method}", seqid, iprot, oprot)
				true
			else
				iprot.skip(Types::STRUCT)
				iprot.read_message_end
				x = ApplicationException.new(ApplicationException::UNKNOWN_METHOD, 'Unknown function '+name)
				oprot.write_message_begin(name, MessageTypes::EXCEPTION, seqid)
				x.write(oprot)
				oprot.write_message_end
				oprot.trans.flush
				false
			end
		end
	end

	module Processor
		def write_result(result, oprot, name, seqid)
			oprot.write_message_begin(@service + ':' + name, MessageTypes::REPLY, seqid)
			result.write(oprot)
			oprot.write_message_end
			oprot.trans.flush
		end
	end
end
