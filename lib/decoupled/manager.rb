# coding: utf-8

####
#
# TODO:
# - über Manager Schedules anlegen & verwalten ?
# - Überwachung der Consumer und Scheduler in den jeweiligen Queues 
#
####

class Decoupled::Manager

  attr_accessor :channels, :msg_conns, :db_conn, :amqp_hosts

  def initialize(options)
    @job_errors     = 0
    @processed_jobs = 0
    @consumer_name  = options[:manager_name]
    @concurrent     = options[:amqp_hosts].length
    @amqp_hosts     = options[:amqp_hosts]
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

#    puts "| Creating AMQP Connections to Hosts: #{@amqp_hosts.join(",")}"
    #puts "| AMQP Fallbacks: #{@amqp_fallbacks.join(",")}" if @amqp_fallbacks.length > 1
#    @msg_conns = amqp_connections
#    puts '| Creating AMQP Channels'
#    puts "|"
#
#    @channels = Array.new
#    @msg_conns.each do |msg_conn|
#      @channels.push msg_conn.createChannel
#      puts "| Using RabbitMQ Client #{msg_conn.getClientProperties["version"]} and RabbitMQ Server #{msg_conn.getServerProperties["version"]}"
#    end

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

      monitoring_consumers

    rescue Exception => e 
      puts "Exception in start_monitoring method => #{e}"
      puts e.backtrace.join("\n")
    end
  end

  def monitoring_consumers
    #@channels.each do |channel|
      @loop = true
      @executor.submit do 
        begin
          while @loop do
            # TOOD: Checks auf die Redis Datenbank vornehmen und aktuelle Schedules überprüfen in MongoDB

            #puts "Checking Consumers and Schedulers on Channel #{}"

            consumers = @redis_conn.hgetall('decoupled.consumers')

            if consumers.empty?
              puts "Keine Consumer gefunden"
            else
              puts "#{consumers.length} Consumer gefunden"
              consumers.each_value { |value|
                consumer    = JSON.parse(value)
                last_answer = Time.now.to_i - consumer['last_answer'].to_i
                if last_answer > 120 # als gecrashed markieren nach 2 Minuten
                  if last_answer > (60*15)*1 # remove redis entry of consumer if there is older than 15 minutes
                    @redis_conn.hdel( 'decoupled.consumers', consumer['name'] )
                  else
                    consumer['status'] = 'crashed'
                    @redis_conn.hset('decoupled.consumers', consumer['name'], consumer.to_json)
                  end
                end
              }
            end

            sleep(5)
          end
        rescue Exception => e 
          puts "Exception in monitoring_queues on Channel => #{e}"
          puts e.backtrace.join("\n")
        end
      end  
    #end
  end

  # All connections need to be closed, otherwise
  # the process will hang and exit will not work.
  def close_connections
    puts 'closing connections'
    @loop = false
    #remove_from_queue_list #unless @no_status
    @executor.shutdown
    #@channels.each { |channel| channel.close }
    #@msg_conns.each { |msg_conn| msg_conn.close }
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
