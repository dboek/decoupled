# coding: utf-8
module Decoupled
	class Consumer
	  #
	  def initialize(consumer_name, channel, queue_name, jobs_per_consumer = 1, process_class)
	    @active_jobs = 0
	    @processed_jobs = 0
	    @job_errors = 0
	    
	    @started_at = Time.now
	    @consumer_name = consumer_name
	    @prefetch_count = jobs_per_consumer
	    @queue_name = queue_name
	
	    @channel = channel
	    @channel.prefetch(@prefetch_count)
	    @channel.on_error(&method(:handle_channel_exception))
	    
	    klass = Object.const_get(process_class)
	    @process_class = klass
	  end
	
	  def start
	    @queue = @channel.queue(@queue_name, :exclusive => false)
	    @queue.subscribe(:ack => true, &self.method(:process_work))
	    listen_for_manager
	  end
	  
	  def listen_for_manager
	    # The Manager calls this queue to check on the workers, they need to answer, if they are still available
	    exchange = @channel.topic("decoupled.worker", :auto_delete => false)
	
	    queue = @channel.queue("", :exclusive => true, :auto_delete => true).bind(exchange)
	    queue.subscribe do |payload|
	      status = {
	        'name' => @consumer_name,
	        'prefetch_count' => @prefetch_count,
	        'active_jobs' => @active_jobs,
	        'processed_jobs' => @processed_jobs,
	        'job_errors' => @job_errors,
	        'last_answer' => Time.now.to_i,
	        'started_at' => @started_at.strftime('%d.%m.%Y %H:%M:%S'),
	      }
	      @channel.direct('').publish( status.to_json, :routing_key => 'decoupled.manager' )
	    end
	  end
	  
	  def process_work(metadata, payload)
	    @active_jobs += 1
	    
	    operation = proc {
	      begin
	        # Later it could be better to have a worker manage more than one message queue and class
	        payload_hash = JSON.parse(payload)
	        @process_class.process_task(payload_hash)
	        @processed_jobs += 1
	      rescue Exception => e
	        @job_errors += 1
	        save_error_to_mongo(payload_hash['api_identifier'], e, payload)
	      end
	    }
	    
	    callback = proc { |result|
	      @active_jobs -= 1
	      @channel.acknowledge(metadata.delivery_tag, false)
	    }
	    
	    EventMachine.defer(operation, callback)
	  end
	  
	  def save_error_to_mongo(api, error, payload)
	    begin
	      # TODO implement it
	    rescue Exception => e
	      # silent error
	    end
	  end
	  
	  def handle_channel_exception(channel, channel_close)
	    puts "Channel-level exception: code = #{channel_close.reply_code}, message = #{channel_close.reply_text}"
	  end
	end
end