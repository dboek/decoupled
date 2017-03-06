# coding: utf-8

class Decoupled::Manager

  attr_accessor :executor, :channel, :msg_conn, :db_conn, :amqp_host

  def initialize(options)
    @job_errors     = 0
    @processed_jobs = 0
    @consumer_name  = options[:manager_name]
    @concurrent     = 1 #options[:concurrent_count]
    #@job_klass      = options[:job_klass]
    @amqp_hosts     = options[:amqp_hosts]
    #@amqp_fallbacks = options[:amqp_fallbacks]
    #@job_count      = 1
    #@msg_queue      = options[:queue_name]
    @redis_host     = options[:redis_host]

    puts "|"
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

    puts "| Creating AMQP Connections to Hosts: #{@amqp_hosts}"
    #puts "| AMQP Fallbacks: #{@amqp_fallbacks.join(",")}" if @amqp_fallbacks.length > 1
    @msg_conns = amqp_connections
    puts '| Creating AMQP Channels'
    puts "|"

    @channels = Array.new
    @msg_conns.each do |msg_conn|
      @channels.push msg_conn.createChannel
      puts "| Using RabbitMQ Client #{@msg_conn.getClientProperties["version"]} and RabbitMQ Server #{@msg_conn.getServerProperties["version"]}"
    end

    # Database Connection
    opts     = MongoClientOptions::Builder.new.connectionsPerHost(@concurrent).build
    port     = options[:environment] == "development" ? 27017 : 27020
    @db_conn = MongoClient.new("localhost:#{port}", opts)

    puts "| Creating MongoDB Pooled Connection on Port: #{port} with #{@db_conn.getMongoOptions.getConnectionsPerHost} Connections per Host"
    puts "| Using MongoDB Java Driver Version #{@db_conn.getVersion}"
  end

  def monitor
    begin
      loop = true

      while loop do 
        puts "Check current Consumer and Scheduler status on RabbitMQ Server X"

        sleep(2)
      end

    rescue Exception => e 
      puts "Exception in monitoring Queues and Consumers => #{e}"
      puts e.backtrace.join("\n")
    end
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
  end

end
