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

	    @count  = java.util.concurrent.atomic.AtomicInteger.new
	    
	    # Instance Objects to be stopped by the decoupled instance
	    puts "|"
	    puts "| Creating ThreadPool of => 2" #"#{@concurrent}"
		@executor  = Executors.newFixedThreadPool(2)
		@no_status = false

	    puts "| Creating AMQP Connection on Host: #{@amqp_host}"
	    if @amqp_fallbacks.length > 1
	      puts "| AMQP Fallbacks: #{@amqp_fallbacks.join(",")}"
	    end
	    @msg_conn = amqp_connection
	    puts '| Creating Channel'
	    @channel = @msg_conn.createChannel

	    # Database Connection
	    opts                    = MongoOptions.new
	    opts.connectionsPerHost = @concurrent
	    if options[:environment] == "development"
	      	@db_conn = Mongo.new( "localhost:27017", opts )
			port     = "27017"
	    else
			@db_conn = Mongo.new( "localhost:27020", opts )
			port     = "27020"
	    end
	    puts "| Creating MongoDB Pooled Connection on Port: #{port}"  
	end

	def start
		begin
		    @autoAck      = false;
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
		    puts '+---------------------------------------------------------------------------------------------'
		    puts ""

		    check_for_new_schedules
		    execute_schedules

		rescue Exception => e 
			puts "Failure in start method of scheduler => #{e}"
		end
	end

	# Thread to save scheduled message in MongoDB
	def check_for_new_schedules
		@executor.submit do
	    	@count.incrementAndGet
			begin
				loop                               = true
				@looking_for_scheduldes_started_at = Time.now

			    while loop do
			        #puts ">> checking for new schedule messages #{Time.now.strftime("%H:%M:%S")}"
			        response_exists = true

			        while response_exists do
			        	response = @channel.basicGet(@msg_queue, @autoAck);

			        	if response
				        	delivery_tag = response.get_envelope.get_delivery_tag
		            		message_body = JSON.parse( String.from_java_bytes(response.getBody()) )
		            		puts "Save Message in Mongodb"
				       		save_schedule_message(message_body)
				       	else
				       		response_exists = false
				        end
			        end

			        #puts "amqp going to sleep #{Time.now.usec}"
			        sleep(10) # Wie lange soll der Thread 
			    end
			rescue Exception => e 
				puts "Failure in Thead check_for_new_schedules => #{e}"
			end
		end
	end

	# Save new schedule in Mongodb
	def save_schedule_message(message)
		begin
			collection_name = @schedule_collections.length > 0 ? @schedule_collections.first : "schedules"
			message         = get_correct_schedule_hash(message)
			doc             = create_document_structure(message)

			@db_conn.getDB(@skydb).getCollection(collection_name).insert(doc)
		rescue Exception => e 
			puts "Failure in saving schedule in collection => #{e}"
		end
	end

	def get_correct_schedule_hash(message)
		schedule_hash = Hash.new

		unless message.has_key? "intervall"
			message.each do |key,value|
				if key == "send_to_queue"
					key = "queue"
				elsif key == "send_in"
					key   = "send_at"
					value = convert_send_in_to_send_at(value)
				elsif key == 'send_weekly_at'
					value = check_at_time(value)
				elsif key == 'send_monthly_at'
					value = check_at_time(value)
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
	def check_at_time(value)
		return_value = value
		time_stamp   = value.split(":")

		if time_stamp.length == 1
			return_value = "#{value}:0000"
		end

		return_value
	end

	# change a send_in message to send_at (this message will be send every full minute)
	def convert_send_in_to_send_at(value)
		time_at_looking_for_schedules = Time.now
		time_to_wait                  = 60 - time_at_looking_for_schedules.strftime("%S").to_i
		time_for_send_in              = time_at_looking_for_schedules + time_to_wait + value
		
		return time_for_send_in.strftime("%Y%m%d%H%M")
	end

	# Thread to send scheduled messages to queue
	def execute_schedules
		@executor.submit do 
			begin
				loop                             = true
				@executing_scheduldes_started_at = Time.now

				wait_for_full_minute = false
				waiting_for_sec      = 60 - @executing_scheduldes_started_at.strftime("%S").to_i
			    if waiting_for_sec > 0
			    	wait_for_full_minute = true
			    end

			    while loop do
			    	if wait_for_full_minute
				    	sleep(waiting_for_sec)
				    	wait_for_full_minute = false
				    else
				    	sleep(60) # Every minute execute scheduled messages
				    end
			        puts ">> checking for executable scheduled messages in db #{Time.now.strftime("%H:%M:%S")}"

			        send_scheduled_message_to_queue(Time.now)

					time_after_processing = Time.now
					waiting_for_sec       = 60 - time_after_processing.strftime("%S").to_i
					if waiting_for_sec > 0
				    	wait_for_full_minute = true
				    end
			    end
			rescue Exception => e 
				puts "Failure in Thead execute_schedules => #{e}"
			end
		end
	end

	# check mongodb for scheduled messages and send it to specific queue
	def send_scheduled_message_to_queue(current_time)
		db = @db_conn.getDB(@skydb)
		db.requestStart()

		send_every_five_minutes       = current_time.strftime("%M").to_i % 5
		send_every_fiveteen_minutes   = current_time.strftime("%M").to_i % 15
		send_every_thirty_minutes     = current_time.strftime("%M").to_i % 30
		send_every_fourtyfive_minutes = current_time.strftime("%M").to_i % 45
		send_every_hour               = current_time.strftime("%M").to_i % 60

		send_daily_at  = current_time.strftime("%H%M").to_i
		send_at        = current_time.strftime("%Y%m%d%H%M").to_i
		send_weekly_at = "#{current_time.wday}:#{current_time.strftime("%H%M")}"

		# February Fix
		send_monthly  = Array.new
		time_tomorrow = current_time + 86400
		if current_time.month == 2
			if time_tomorrow.month != 2
				if current_time.mday == 28
					send_monthly.push "28:#{current_time.strftime("%H%M")}"
				end
				send_monthly.push "29:#{current_time.strftime("%H%M")}"
				send_monthly.push "30:#{current_time.strftime("%H%M")}"
				send_monthly.push "31:#{current_time.strftime("%H%M")}"
			end
		else
			# Fix for every months with only 30 days
			if time_tomorrow.month != current_time.month and current_time.month % 2 == 0
				send_monthly.push "30:#{current_time.strftime("%H%M")}"
				send_monthly.push "31:#{current_time.strftime("%H%M")}"
			else
				send_monthly.push "#{current_time.mday.to_s}:#{current_time.strftime("%H%M")}"
			end
		end

		puts "---------------------------------"
		puts "Searching in Collection #{@schedule_collections.join(",")}"
		for collection in @schedule_collections
		    result_documents = Array.new

			if send_every_five_minutes == 0
				find_query = BasicDBObject.new
				find_query.put("send_every", "5")
				result_document = db.getCollection(collection).find(find_query)  
				while(result_document.hasNext())
				    result_documents.push result_document.next()
				end
			end

			if send_every_fiveteen_minutes == 0
				find_query = BasicDBObject.new
				find_query.put("send_every", "15")
				result_document = db.getCollection(collection).find(find_query)
				while(result_document.hasNext())
				    result_documents.push result_document.next()
				end  
			end

			if send_every_thirty_minutes == 0
				find_query = BasicDBObject.new
				find_query.put("send_every", "30")
				result_document = db.getCollection(collection).find(find_query)  
				while(result_document.hasNext())
				    result_documents.push result_document.next()
				end
			end

			if send_every_fourtyfive_minutes == 0
				find_query = BasicDBObject.new
				find_query.put("send_every", "45")
				result_document = db.getCollection(collection).find(find_query)  
				while(result_document.hasNext())
				    result_documents.push result_document.next()
				end
			end

			if send_every_hour == 0
				find_query = BasicDBObject.new
				find_query.put("send_every", "60")
				result_document = db.getCollection(collection).find(find_query)  
				while(result_document.hasNext())
				    result_documents.push result_document.next()
				end
			end

			find_query = BasicDBObject.new
			find_query.put("send_at", send_at.to_s)
			result_document = db.getCollection(collection).find(find_query) 
			while(result_document.hasNext())
			    result_documents.push result_document.next()
			end 
			# remove useless schedule from collection (only for send_at messages)
			db.getCollection(collection).remove(find_query)

			find_query = BasicDBObject.new
			find_query.put("send_daily_at", send_daily_at.to_s)
			result_document = db.getCollection(collection).find(find_query)  
			while(result_document.hasNext())
			    result_documents.push result_document.next()
			end

			find_query = BasicDBObject.new
			find_query.put("send_weekly_at", send_weekly_at.to_s)
			result_document = db.getCollection(collection).find(find_query) 
			while(result_document.hasNext())
			    result_documents.push result_document.next()
			end 

			for send_monthly_at in send_monthly
				find_query = BasicDBObject.new
				find_query.put("send_monthly_at", send_monthly_at.to_s)
				result_document = db.getCollection(collection).find(find_query) 
				while(result_document.hasNext())
				    result_documents.push result_document.next()
				end 
			end

			for document in result_documents
				begin
					puts "--> Send Payload to #{document["queue"]}"
					@channel.basicPublish("", document["queue"], nil, document["payload"].to_json.to_java_bytes)
				rescue Exception => e
					puts "Failure in sending scheduled message to #{document["queue"]}"
				end
			end
		end       
		puts "---------------------------------"
		puts ""

		db.requestDone() 
	end

	# Create document structure for scheduled message
    def create_document_structure(doc)
        # Create Document Structure for BasicDBObject
        return_document = BasicDBObject.new

        if doc.class.to_s == "Hash"
            doc.each do |key, value|
                if key.to_s.include? "$"
                    if value.class.to_s == "Hash"
                        #if "#{key}" == "$push"
                        #    return_document.put("$addToSet", create_document_structure(value))
                        #else
                            return_document.put("#{key}", create_document_structure(value))
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
                        return_document.put("#{key}", create_document_structure(value))
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
  	  unless @no_status
  	  #  remove_from_queue_list
  	  end
  	  @executor.shutdown
  	  @channel.close
  	  @msg_conn.close
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