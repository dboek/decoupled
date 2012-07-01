# coding: utf-8
class Decoupled::Consumer

  attr_accessor :executor, :channel, :msg_conn, :db_conn, :concurrent, :job_klass

  def initialize(concurrent, job_klass)
    @concurrent = concurrent
    @job_klass  = job_klass
    @job_count  = 1

    @count  = java.util.concurrent.atomic.AtomicInteger.new
    
    # Instance Objects to be stopped by the decoupled instance
    puts "Creating ThreadPool of => #{@concurrent}"
    @executor = Executors.newFixedThreadPool(@concurrent)
    
    puts 'Creating AMQP Connection'
    @msg_conn = amqp_connection
    puts 'Creating Channel'
    @channel = @msg_conn.createChannel

    # Database Connection
    opts    = MongoOptions.new
    opts.connectionsPerHost = @concurrent
    puts 'Creating MongoDB Pooled Connection'
    @db_conn  = Mongo.new( "localhost:27017", opts )
  end

  def do_work(payload)
    @executor.submit do
      @count.incrementAndGet

      begin
        work = Decoupled::Worker.new(@count, @db_conn, payload)
        work.execute(@job_klass)

      rescue Exception => e
        @count.decrementAndGet
        puts e
      end

    end
  end

  def start
    begin
      # Consumer subscription to queue
      autoAck = false;
      exchangeName  = 'livesearch'
      queueName     = 'livesearch'
      routingKey    = ''

      puts "binding channel to => #{exchangeName}"
      @channel.exchangeDeclare(exchangeName, "direct", true)
      @channel.queueBind(queueName, exchangeName, routingKey)

      loop = true

      while loop do
        puts "worker threads busy #{@count.get}"

        while @count.get < @concurrent do
          puts '=> checking for new messages'
          response = @channel.basicGet(queueName, autoAck);

          if not response
            # No message retrieved.
            sleep(3)
          else
            #AMQP.BasicProperties props = response.getProps();
            delivery_tag = response.get_envelope.get_delivery_tag
            message_body = JSON::parse( String.from_java_bytes(response.getBody()) )
            puts @job_count
            do_work(message_body)

            @channel.basicAck(delivery_tag, false)
            @job_count += 1
          end
        end
        puts "amqp going to sleep #{Time.now.usec}"
        sleep(10)
      end

    rescue Exception => e
      puts e
    end
  end

  # All connections need to be closed, otherwise
  # the process will hang and exit will not work.
  def close_connections
    puts 'closing connections'
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
    #factory.setHost(hostName)
    #factory.setPort(portNumber)
    
    factory.newConnection
  end

end