# coding: utf-8

class Decoupled::Manager

  attr_accessor :channels, :msg_conns, :db_conn, :amqp_hosts

  def initialize(options)
    @job_errors     = 0
    @processed_jobs = 0
    @consumer_name  = options[:manager_name]
    @concurrent     = options[:amqp_hosts].length #1 #options[:concurrent_count]
    #@job_klass      = options[:job_klass]
    @amqp_hosts     = options[:amqp_hosts]
    #@amqp_fallbacks = options[:amqp_fallbacks]
    #@job_count      = 1
    #@msg_queue      = options[:queue_name]
    @redis_host     = options[:redis_host]

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

    puts "| Creating AMQP Connections to Hosts: #{@amqp_hosts.join(",")}"
    #puts "| AMQP Fallbacks: #{@amqp_fallbacks.join(",")}" if @amqp_fallbacks.length > 1
    @msg_conns = amqp_connections
    puts '| Creating AMQP Channels'
    puts "|"

    @channels = Array.new
    @msg_conns.each do |msg_conn|
      @channels.push msg_conn.createChannel
      puts "| Using RabbitMQ Client #{msg_conn.getClientProperties["version"]} and RabbitMQ Server #{msg_conn.getServerProperties["version"]}"
    end

    # Database Connection
    opts     = MongoClientOptions::Builder.new.connectionsPerHost(@concurrent).build
    port     = options[:environment] == "development" ? 27017 : 27020
    @db_conn = MongoClient.new("localhost:#{port}", opts)

    puts "| Creating MongoDB Pooled Connection on Port: #{port} with #{@db_conn.getMongoOptions.getConnectionsPerHost} Connections per Host"
    puts "| Using MongoDB Java Driver Version #{@db_conn.getVersion}"
  end

  def start_monitoring
    begin
      puts "|"
      puts "| Manager ready for monitoring now"
      puts '+----------------------------------------------------------------------------------------------'
      puts ""

      @init_manager = Time.now
      @last_answer  = @init_manager

      monitoring_queues

    rescue Exception => e 
      puts "Exception in start_monitoring method => #{e}"
      puts e.backtrace.join("\n")
    end
  end

  def monitoring_queues
    @channels.each do |channel|
      @executor.submit do 
        loop = true

        begin
          while loop do
            # TOOD: Checks auf die Redis Datenbank vornehmen und aktuelle Schedules überprüfen in MongoDB

            puts "Checking Consumers and Schedulers on Channel #{}"

            sleep(5)
          end
        rescue Exception => e 
          puts "Exception in monitoring_queues on Channel => #{e}"
          puts e.backtrace.join("\n")
        end
      end  
    end
  end

  # All connections need to be closed, otherwise
  # the process will hang and exit will not work.
  def close_connections
    puts 'closing connections'
    #remove_from_queue_list #unless @no_status
    @executor.shutdown
    @channels.each { |channel| channel.close }
    @msg_conns.each { |msg_conn| msg_conn.close }
  end

  private

  # @return Connection to RabbitMQ Server
  def amqp_connections
    connections = Array.new
    @amqp_hosts.each do |amqp_host|
      factory = ConnectionFactory.new 
      #factory.setUsername(userName)
      #factory.setPassword(password)
      #factory.setVirtualHost(virtualHost)
      factory.setHost(amqp_host)
      #factory.setPort(portNumber)
      #factory.setNetworkRecoveryInterval(10000);
      connections.push factory.newConnection
    end
    return connections
  end

end
