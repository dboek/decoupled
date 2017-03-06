# coding: utf-8
class Decoupled::Consumer

  attr_accessor :executor, :channel, :msg_conn, :db_conn, :concurrent, :job_klass, :amqp_host

  # Since java.util.concurrent.ThreadPoolExecutor.submit has multiple signatures,
  # we force JRuby to use the one signature we want, and stop a warning from being emitted.
  java.util.concurrent.ThreadPoolExecutor.class_eval do
    java_alias :submit, :submit, [java.util.concurrent.Callable.java_class]
  end

  def initialize(options)
    @job_errors     = 0
    @processed_jobs = 0
    @consumer_name  = options[:consumer_name]
    @concurrent     = options[:concurrent_count]
    @job_klass      = options[:job_klass]
    @amqp_host      = options[:amqp_host]
    @amqp_fallbacks = options[:amqp_fallbacks]
    @job_count      = 1
    @msg_queue      = options[:queue_name]
    @redis_host     = options[:redis_host]

    @count = java.util.concurrent.atomic.AtomicInteger.new
    
    # Instance Objects to be stopped by the decoupled instance
    puts "|"
    puts "| Creating ThreadPool of => #{@concurrent}"
    @executor = Executors.newFixedThreadPool(@concurrent)

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
  end

  def do_work(payload)
    @executor.submit do
      begin
        @count.incrementAndGet
        puts ">>> INCREMENT CURRENT JOBS #{@count}"
        work = Decoupled::Worker.new(@count, @db_conn, @redis_conn, payload)
        work.execute(@job_klass)
        @processed_jobs += 1
      rescue Exception => e
        @count.decrementAndGet
        puts ">>> EXCEPTION IN WORKER THREAD"
        puts ">>> DECREMENT CURRENT JOBS #{@count}"
        puts e
      end

    end
  end

  def start
    begin
      # Consumer subscription to queue
      autoAck      = false;
      exchangeName = @msg_queue
      queueName    = @msg_queue
      routingKey   = ''

      puts "|"
      puts "| binding channel to => #{exchangeName}"

      @channel.exchangeDeclare(exchangeName, "direct", true)
      @channel.queueDeclare(queueName, false, false, false, nil)
      @channel.queueBind(queueName, exchangeName, routingKey)

      puts "|"
      puts "| Consumer ready for work now"
      puts '+---------------------------------------------------------------------------------------------'
      puts ""

      loop = true

      @started_at    = Time.now
      @last_answer   = @started_at
      @init_consumer = @started_at
      listen_for_manager

      while loop do
        puts "worker threads busy #{@count.get}"

        while @count.get < @concurrent do
          response = @channel.basicGet(queueName, autoAck);

          @last_answer = Time.now
          if (@last_answer.to_i - @init_consumer.to_i) > 30
            @init_consumer = @last_answer
            listen_for_manager
          end

          if not response
            # No message retrieved.
            sleep(3)
          else
            #AMQP.BasicProperties props = response.getProps();
            delivery_tag = response.get_envelope.get_delivery_tag
            message_body = JSON.parse( String.from_java_bytes(response.getBody()) )
            #puts "processed Jobs: #{@job_count}"
            do_work(message_body)

            @channel.basicAck(delivery_tag, false)
            @job_count += 1
          end
        end
        puts "amqp going to sleep #{Time.now.utc} | #{@count.get}" #usec}"
        sleep(10)
      end

    rescue Exception => e
      puts e
    end
  end

  # Saving current status of the consumer
  def listen_for_manager
    consumer_status = @count.get > 0 ? "working" : "running"
    status = {
      'name'           => @consumer_name,
      'queue_name'     => @msg_queue,
      'prefetch_count' => @concurrent,
      'active_jobs'    => @count.get,
      'processed_jobs' => @processed_jobs,
      'job_errors'     => @job_errors,
      'last_answer'    => @last_answer.to_i,
      'status'         => consumer_status,
      'started_at'     => @started_at.strftime('%d.%m.%Y %H:%M:%S'),
    }
    @redis_conn.hset('decoupled.consumers', @consumer_name, status.to_json) unless @no_status
  end

  # Remove Consumer from redis queue list while closing connection
  def remove_from_queue_list
    puts "Removing #{@consumer_name} from Queue List"
    @redis_conn.hdel('decoupled.consumers', @consumer_name)
  end

  # All connections need to be closed, otherwise
  # the process will hang and exit will not work.
  def close_connections
    puts 'closing connections'
    remove_from_queue_list unless @no_status
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
    #factory.setNetworkRecoveryInterval(10000);
    
    factory.newConnection
  end

end
