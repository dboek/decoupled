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
	    #@schedule_executor = Executors.newCachedThreadPool(1)
	    #@schedule_requests = Executors.newCachedThreadPool(1)
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

	# Save Message in MongoDB
	def looking_for_new_schedules
		@executor.submit do
	    	@count.incrementAndGet
			begin
				loop           = true
				@started_at    = Time.now
				@last_answer   = @started_at
				@init_consumer = @started_at
			    #listen_for_manager

			    while loop do
			        puts ">> checking for new schedule messages #{Time.now.strftime("%H:%M:%S")}"
			        response = @channel.basicGet(@msg_queue, @autoAck);

			        @last_answer = Time.now
			        if (@last_answer.to_i - @init_consumer.to_i) > 30
			          @init_consumer = @last_answer
			          #listen_for_manager
			        end

			        if response
			        	delivery_tag = response.get_envelope.get_delivery_tag
	            		message_body = JSON.parse( String.from_java_bytes(response.getBody()) )
			       		save_schedule_message(message_body)
			        end

			        puts "amqp going to sleep #{Time.now.usec}"
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
			collection_name = message["send_to_queue"]
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
					key = "send_at"
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

	def execute_schedules
		@executor.submit do 
			begin
				loop           = true
				@started_at    = Time.now
				@last_answer   = @started_at
				@init_consumer = @started_at
			    #listen_for_manager

			    while loop do
			        #puts ">> worker thread execute_schedules busy"
			        puts ">> checking for executable scheduled messages in db #{Time.now.strftime("%H:%M:%S")}"

			        @last_answer = Time.now
			        if (@last_answer.to_i - @init_consumer.to_i) > 30
			          @init_consumer = @last_answer
			          #listen_for_manager
			        end

			        # send_schedule_to_queue
			  
			        puts "amqp going to sleep #{Time.now.usec}"
			        sleep(20)
			    end
			rescue Exception => e 
				puts "Failure in Thead execute_schedules => #{e}"
			end
		end
	end

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