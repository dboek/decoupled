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
	    @executor = Executors.newFixedThreadPool(2)

	    @no_status = false
	    #if @redis_host != ""
	    #  puts "| Creating Redis Connection on Host: #{@redis_host}"
	    #  begin
	    #    @redis_conn = Redis.new(:host => @redis_host) 
	    #  rescue Exception => e
	    #    puts "Unable to connect to #{@redis_host} => #{e}"
	    #  end
	    #else
	    #  @no_status = true
	    #end

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
			port   = "27017"
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

		    looking_for_new_schedules
		    execute_schedules

		rescue Exception => e 
			puts "Failure in start method of scheduler => #{e}"
		end
	end

	# Thread to save scheduled message in MongoDB
	def looking_for_new_schedules
		@executor.submit do
	    	@count.incrementAndGet
			begin
				loop                               = true
				@looking_for_scheduldes_started_at = Time.now

				@last_answer                       = @looking_for_scheduldes_started_at
				@init_consumer                     = @looking_for_scheduldes_started_at
			    #listen_for_manager

			    while loop do
			        #puts ">> checking for new schedule messages #{Time.now.strftime("%H:%M:%S")}"
			        response = @channel.basicGet(@msg_queue, @autoAck);

			        #@last_answer = Time.now
			        #if (@last_answer.to_i - @init_consumer.to_i) > 30
			        #  @init_consumer = @last_answer
			        #  #listen_for_manager
			        #end
			        
			        if response
			        	delivery_tag = response.get_envelope.get_delivery_tag
	            		message_body = JSON.parse( String.from_java_bytes(response.getBody()) )
			       		save_schedule_message(message_body)
			        end

			        #puts "amqp going to sleep #{Time.now.usec}"
			        sleep(10) # Wie lange soll der Thread 
			    end
			rescue Exception => e 
				puts "Failure in Thead looking_for_new_schedules => #{e}"
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

		#{
		# '_id': bsonobject,
		# 'send_every': 5,
		# 'send_at': '201209281200'
		# 'send_daily_at': '1200'
		# 'queue': 'pricealert',
		# 'payload': {...}
		#}

		unless message.has_key? "intervall"
			message.each do |key,value|
				if key == "send_to_queue"
					key = "queue"
				elsif key == "send_in"
					key   = "send_at"
					value = change_send_in_to_send_at_value(value)
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

	# change a send_in message to send_at (this message will be send every full minute)
	def change_send_in_to_send_at_value(value)
		time_at_looking_for_schedules = Time.now
		time_to_wait                  = 60 - time_at_looking_for_schedules.strftime("%S").to_i
		time_for_send_in              = time_at_looking_for_schedules + time_to_wait + value

		#puts "TIME FOR SEND IN:"
		#puts time_for_send_in.strftime("%H:%M:%S")
		#puts "-----------------"

		time_for_send_in.strftime("%Y%m%d%H%M")
	end

	# Thread to send scheduled messages to queue
	def execute_schedules
		@executor.submit do 
			begin
				loop                             = true
				@executing_scheduldes_started_at = Time.now

				@last_answer    = @executing_scheduldes_started_at
				@init_scheduler = @executing_scheduldes_started_at
			    #listen_for_manager

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

					@current_time = Time.now
					#@last_answer  = Time.now
				    #if (@last_answer.to_i - @init_scheduler.to_i) > 30
				    #  @init_scheduler = @last_answer
				    #  #listen_for_manager
				    #end

			        send_scheduled_message_to_queue

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

	def send_scheduled_message_to_queue
		db = @db_conn.getDB(@skydb)
		db.requestStart()

		send_every_five_minutes       = @current_time.strftime("%M").to_i % 5
		send_every_fiveteen_minutes   = @current_time.strftime("%M").to_i % 15
		send_every_thirty_minutes     = @current_time.strftime("%M").to_i % 30
		send_every_fourtyfive_minutes = @current_time.strftime("%M").to_i % 45
		send_every_hour               = @current_time.strftime("%M").to_i % 60

		send_daily_at = @current_time.strftime("%H%M").to_i
		send_at       = @current_time.strftime("%Y%m%d%H%M").to_i

		#puts "Dokumente mit folgenden Zeitstempeln: "
		#puts send_at
		#puts send_daily_at
		#puts send_every_five_minutes       
		#puts send_every_fiveteen_minutes   
		#puts send_every_thirty_minutes     
		#puts send_every_fourtyfive_minutes 
		#puts send_every_hour  

		for collection in @schedule_collections
			puts "---------------------------------"
			puts "Suche in Collection #{collection}"

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
			find_query.put("send_daily_at", send_daily_at.to_s)
			result_document = db.getCollection(collection).find(find_query)  
			while(result_document.hasNext())
			    result_documents.push result_document.next()
			end

			find_query = BasicDBObject.new
			find_query.put("send_at", send_at.to_s)
			result_document = db.getCollection(collection).find(find_query) 
			while(result_document.hasNext())
			    result_documents.push result_document.next()
			end 

			for document in result_documents
				begin
					puts "--> Send Payload to #{document["queue"]}"
					@channel.basicPublish("", document["queue"], nil, document["payload"].to_json.to_java_bytes)
				rescue Exception => e
					puts "Failure in sending scheduled message to #{document["queue"]}"
				end
			end

			puts "---------------------------------"
			puts ""
		end       

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