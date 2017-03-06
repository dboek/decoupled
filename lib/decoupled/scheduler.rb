# coding: utf-8
class Decoupled::Scheduler

	attr_accessor :executor, :channel, :msg_conn, :db_conn, :concurrent, :amqp_host

	def initialize(options)
		@job_errors           = 0
		@processed_jobs       = 0
		@consumer_name        = options[:scheduler_name]
		@schedule_collections = options[:collections]

	  @concurrent     = options[:concurrent_count]
	  @amqp_host      = options[:amqp_host]
	  @amqp_fallbacks = options[:amqp_fallbacks]
	  @job_count      = 1
	  @msg_queue      = "schedule"
	  @redis_host     = options[:redis_host]
	  @skydb          = options[:scheduler_db]
	    
	  # Instance Objects to be stopped by the decoupled instance
	  puts "|"
	  puts "| Creating ThreadPool of => #{@concurrent}"
		@executor  = Executors.newFixedThreadPool(@concurrent)

		@no_status = false
		if @redis_host != ""
			puts "| Creating Redis Connection on Host: #{@redis_host}"
			begin
				@redis_conn = Redis.new(:host => @redis_host) 
			rescue Exception => e
				puts "Unable to connect to #{@redis_host} => #{e}"
				@no_status = true
			end
		else
			@redis_conn = nil
			@no_status  = true
		end

	  puts "| Creating AMQP Connection on Host: #{@amqp_host}"
		puts "| AMQP Fallbacks: #{@amqp_fallbacks.join(",")}" if @amqp_fallbacks.length > 1
	  @msg_conn = amqp_connection
	  puts '| Creating AMQP Channel'
	  puts "|"
	  @channel = @msg_conn.createChannel
	  puts "| Using RabbitMQ Client #{@msg_conn.getClientProperties["version"]} and RabbitMQ Server #{@msg_conn.getServerProperties["version"]}"

	  # Database Connection
	  opts     = MongoClientOptions::Builder.new.connectionsPerHost(@concurrent).build
    port     = options[:environment] == "development" ? 27017 : 27020
    @db_conn = MongoClient.new("localhost:#{port}", opts)

	  puts "| Creating MongoDB Pooled Connection on Port: #{port} with #{@db_conn.getMongoOptions.getConnectionsPerHost} Connections per Host" 
	  puts "| Using MongoDB Java Driver Version #{@db_conn.getVersion}"

	  @loop = true
	end

	def start
		begin
			@autoAck     = false;
			exchangeName = @msg_queue
			queueName    = @msg_queue
			routingKey   = ''

			puts "|"
			puts "| binding channel to => #{exchangeName}"

			@channel.exchangeDeclare(exchangeName, "direct", true)
			@channel.queueDeclare(queueName, false, false, false, nil)
			@channel.queueBind(queueName, exchangeName, routingKey)

			puts "|"
			puts "| Scheduler ready for work now"
			puts '+----------------------------------------------------------------------------------------------'
			puts ""

			@init_scheduler 		 = Time.now
			@last_answer    		 = @init_scheduler
			@processed_schedules = 0
			@queued_schedules		 = 0

			handle_incoming_scheduled_messages
			handle_sending_scheduled_messages

		rescue Exception => e 
			puts "Failure in start method of scheduler => #{e}"
		end
	end

	# Thread to save scheduled message in MongoDB
	def handle_incoming_scheduled_messages
		@executor.submit do
			begin
				while @loop do
					#puts ">> checking for new scheduled messages #{Time.now.strftime("%H:%M:%S")}"
					response_exists = true

					# status for manager
					@last_answer = Time.now
					listen_for_manager

					while response_exists do
						response = @channel.basicGet(@msg_queue, @autoAck);

						if response
							puts ">> get new scheduled message at #{Time.now.strftime("%H:%M:%S")}"
					  	delivery_tag = response.get_envelope.get_delivery_tag
					  	message_body = JSON.parse( String.from_java_bytes(response.getBody()) )

					 		save_scheduled_message_in_db(message_body)
					 		@channel.basicAck(delivery_tag, false)
					 	else
					 		response_exists = false
					  end
					end

					sleep(10) # Thread going to sleep for 10 secs
				end
			rescue Exception => e 
				puts "Exception in Thread handle_incoming_scheduled_messages => #{e}"
			end
		end
	end

	# Save new scheduled message in database
	def save_scheduled_message_in_db(message)
		begin
			collection_name = @schedule_collections.length > 0 ? @schedule_collections.first : "schedules"
			message         = convert_scheduled_message_to_hash_for_db(message)
			doc             = create_document_structure_for_db(message)

			@db_conn.getDB(@skydb).getCollection(collection_name).insert(doc)
			@queued_schedules += 1
			#puts "   => Save Message #{message.inspect} in Database #{collection_name}"
		rescue Exception => e 
			puts "Failure in saving schedule in collection => #{e}"
			puts e.backtrace.join("\n")
		end
	end

	def convert_scheduled_message_to_hash_for_db(message)
		schedule_hash = Hash.new

		unless message.has_key? "intervall"
			message.each do |key,value|
				if key == "send_to_queue"
					key = "queue"
				elsif key == "send_in"
					key   = "send_at"
					value = handle_send_in(value)
				elsif key == 'send_weekly_at'
					value = get_timestamp(value)
				elsif key == 'send_monthly_at'
					value = get_timestamp(value)
				end
				schedule_hash[key] = value
			end
		else
			message.each do |key,value|
				schedule_hash[key] = value
			end
		end

		return schedule_hash
	end

	# check for send_weekly_at and send_monthly_at to set standard time to midnight
	def get_timestamp(value)
		return_value = value
		time_stamp   = value.split(":")

		if time_stamp.length == 1
			return_value = "#{value}:0000"
		end

		return_value
	end

	# change a send_in message to send_at (this message will be send every full minute)
	def handle_send_in(value)
		time_at_looking_for_schedules = Time.now
		time_to_wait                  = 60 - time_at_looking_for_schedules.strftime("%S").to_i
		time_for_send_in              = time_at_looking_for_schedules + time_to_wait + value.to_i
		
		return time_for_send_in.strftime("%Y%m%d%H%M")
	end

	# Thread to send scheduled messages to queue
	def handle_sending_scheduled_messages
		@executor.submit do 
			begin
				wait_for_full_minute = false
				waiting_for_sec      = 60 - Time.now.strftime("%S").to_i
				wait_for_full_minute = true if waiting_for_sec > 0

				while @loop do
					if wait_for_full_minute
				  	sleep(waiting_for_sec)
				  	wait_for_full_minute = false
				  else
				  	#sleep(60) # Every minute execute scheduled messages
				  	sleep(2)
				  end
					#puts ">> check for sending scheduled messages to specific queues #{Time.now.strftime("%H:%M:%S")}"

					send_scheduled_messages_to_queue(Time.now)

					waiting_for_sec 		 = 60 - Time.now.strftime("%S").to_i
				 	wait_for_full_minute = true if waiting_for_sec > 0
				end
			rescue Exception => e 
				puts "Exception in Thread handle_sending_scheduled_messages => #{e}"
			end
		end
	end

	# check mongodb for scheduled messages and send it to specific queue
	def send_scheduled_messages_to_queue(current_time)
		db = @db_conn.getDB(@skydb)
		db.requestStart()

		current_minutes                        = current_time.strftime("%M").to_i
		current_hour_time 										 = current_time.strftime("%H%M")
		send_every_five_minutes_interval       = current_minutes % 5
		send_every_fiveteen_minutes_interval   = current_minutes % 15
		send_every_thirty_minutes_interval     = current_minutes % 30
		send_every_fourtyfive_minutes_interval = current_minutes % 45
		send_every_hour_interval               = current_minutes % 60

		send_daily_at_interval  = current_hour_time
		send_at_interval        = current_time.strftime("%Y%m%d%H%M").to_i
		send_weekly_at_interval = "#{current_time.wday}:#{current_hour_time}"
		send_monthly_interval   = Array.new
		time_of_tomorrow        = current_time + 86400

		# February Fix
		if current_time.month == 2
			if time_of_tomorrow.month != 2
				if current_time.mday == 28
					send_monthly_interval.push "28:#{current_hour_time}"
				end
				send_monthly_interval.push "29:#{current_hour_time}"
				send_monthly_interval.push "30:#{current_hour_time}"
				send_monthly_interval.push "31:#{current_hour_time}"
			end
		else
			# Fix for months with 30 days
			if time_of_tomorrow.month != current_time.month and current_time.month % 2 == 0
				send_monthly_interval.push "30:#{current_hour_time}"
				send_monthly_interval.push "31:#{current_hour_time}"
			else
				send_monthly_interval.push "#{current_time.mday.to_s}:#{current_hour_time}"
			end
		end
		
		result_documents = Array.new
		@schedule_collections.each do |collection|
			or_query_list    = Array.new
			query            = BasicDBObject.new
			query.put("send_at", send_at_interval.to_s)
			or_query_list.push query
			
			query = BasicDBObject.new
			query.put("send_daily_at", send_daily_at_interval.to_s)
			or_query_list.push query
			query = BasicDBObject.new
			query.put("send_weekly_at", send_weekly_at_interval.to_s)
			or_query_list.push query
			send_monthly_interval.each do |send_monthly_at|
				query = BasicDBObject.new
				query.put("send_monthly_at", send_monthly_at.to_s)
				or_query_list.push query
			end

			if send_every_five_minutes_interval == 0
				query = BasicDBObject.new
				query.put("send_every", "5")
				or_query_list.push query

				if send_every_fiveteen_minutes_interval == 0
					query = BasicDBObject.new
					query.put("send_every", "15")
					or_query_list.push query
					if send_every_thirty_minutes_interval == 0
						query = BasicDBObject.new
						query.put("send_every", "30")
						or_query_list.push query
						if send_every_fourtyfive_minutes_interval == 0
							query = BasicDBObject.new
				  		query.put("send_every", "45")
				  		or_query_list.push query
				  		if send_every_hour_interval == 0
				  			query = BasicDBObject.new
				    		query.put("send_every", "60")
				    		or_query_list.push query
				    	end
				  	end
					end
				end
			end
			or_query = BasicDBObject.new
			or_query.put("$or", or_query_list);

			result_document = db.getCollection(collection).find(or_query)  
			while(result_document.hasNext())
				result_documents.push result_document.next()
			end

			# remove useless schedule from collection (only for send_at messages)
			find_query = BasicDBObject.new
			find_query.put("send_at", send_at_interval.to_s)
			db.getCollection(collection).remove(find_query)
		end

		unless result_documents.empty?
			puts "   ------------------------------------"
			puts "   | Searching in Collection #{@schedule_collections.join(",")}"
			puts "   |"
			result_documents.each do |document|
				begin
					puts "   | Send Payload"
					request = {}
					document['payload'].each { |key, value| request[key] = get_hash(value) }
					@channel.basicPublish("", document["queue"], nil, request.to_json.to_java_bytes)
					puts "   | #{request.to_json.inspect} to Queue: #{document["queue"]}"
					puts "   |"
					@processed_schedules += 1
				rescue Exception => e
					puts "Failure in sending scheduled message to #{document["queue"]}"
					puts "#{e.backtrace.join("\n")}"
				end
			end
			puts "   ------------------------------------"
		end
		db.requestDone() 
	end

	# change mongodb datastructure to normal ruby hash object
	def get_hash(doc)
		if doc.class.to_s == "String"
			return_hash = doc
		else
			return_hash = Hash.new
			doc.each do |key, value|
				if value.class.to_s == "Java::ComMongodb::BasicDBObject"
					return_hash[key] = get_hash(value)
				elsif value.class.to_s == "Java::ComMongodb::BasicDBList"
					return_hash[key] = Array.new
					value.each { |val| return_hash[key].push get_hash(val) }
				else
					return_hash[key] = value
				end
			end
		end

		return return_hash
	end

	# Create document structure of scheduled message for database
	def create_document_structure_for_db(doc)
		# Create Document Structure for BasicDBObject
		return_document = BasicDBObject.new
		if doc.class.to_s == "Hash"
			doc.each do |key, value|
				if key.to_s.include? "$"
					if value.class.to_s == "Hash"
						#if "#{key}" == "$push"
						#    return_document.put("$addToSet", create_document_structure(value))
						#else
						    return_document.put("#{key}", create_document_structure_for_db(value))
						#end
					else
						#if "#{key}" == "$push"
						#    return_document.put("$addToSet", value) 
						#else
						    return_document.put("#{key}", value) 
						#end
					end
				else
					if value.class.to_s == "Hash"
						return_document.put("#{key}", create_document_structure_for_db(value))
					elsif value.class.to_s == "Array"
						return_document.put("#{key}", value)    
					else
						return_document.put("#{key}", value) 
					end
				end
			end
		end
		return_document
	end

	# All connections need to be closed, otherwise
	# the process will hang and exit will not work.
	def close_connections
		puts 'closing connections'
		@loop = false
		remove_from_queue_list unless @no_status
		@executor.shutdown
		@channel.close
		@msg_conn.close
	end

	# saving current status of scheduler
  def listen_for_manager
  	begin
	    status = {
	      'name'           			=> @consumer_name,
	      'queue_name'     			=> @msg_queue,
	      'prefetch_count' 			=> @concurrent,
	      'queued_schedules'   	=> @queued_schedules,
	      'processed_schedules' => @processed_schedules,
	      'last_answer'    			=> @last_answer.to_i,
	      'status'         			=> "working",
	      'started_at'     			=> @init_scheduler.strftime('%d.%m.%Y %H:%M:%S'),
	    }
	    @redis_conn.hset('decoupled.schedulers', @consumer_name, status.to_json) unless @no_status
	  rescue Exception => e
	  	puts "Exception in listen_for_manager => #{e}"
	  	puts e.backtrace.join("\n")
	  end
  end

  # Remove Consumer from redis queue list while closing connection
  def remove_from_queue_list
    puts "Removing #{@consumer_name} from Queue List"
    @redis_conn.hdel('decoupled.schedulers', @consumer_name)
  end

	private

	# @return Connection to RabbitMQ Server
	def amqp_connection
		factory = ConnectionFactory.new 
		#factory.setUsername(userName)
		#factory.setPassword(password)
		#factory.setVirtualHost(virtualHost)
		factory.setHost(@amqp_host)
		#factory.setPort(portNumber)
		
		factory.newConnection
	end

end
