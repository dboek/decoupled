# coding: utf-8
module Decoupled
	class Manager
	  
	  def initialize(channel)
	    @redis = Redis.new
	    
	    @channel = channel
	    @channel.on_error(&method(:handle_channel_exception))
	  end
	
	  def start
	    @queue = @channel.queue('decoupled.manager', :exclusive => false)
	    @queue.subscribe(:ack => true, &self.method(:process_work))
	    
	    # Kickoff a timer asking every 30 seconds for an update of every worker
	    timer = EventMachine::PeriodicTimer.new(30) do
	      exchange = @channel.topic("decoupled.worker", :auto_delete => false)
	      exchange.publish( { 'question' => 'status'}.to_json )
	      mark_crashed_consumers
	    end
	  end
	  
	  def process_work(metadata, payload)
	    # Answering questions from workers
	    # stop/start worker watch
	    consumer = JSON.parse(payload)
	    
	    if consumer['name']
	      set_consumer_status(consumer)
	      @channel.acknowledge(metadata.delivery_tag, false)
	    else
	      @channel.acknowledge(metadata.delivery_tag, false)
	    end
	    
	  end
	  
	  def mark_crashed_consumers
	    consumers = @redis.hgetall('decoupled.consumers')
	    consumers.each_value { |value|
	      consumer = JSON.parse(value)
	      last_answer = Time.now.to_i - consumer['last_answer'].to_i
	      if last_answer > 60
	        if last_answer > (60*60)*12 # älter als 12 Stunden dann löschen
	          @redis.hdel( 'decoupled.consumers', consumer['name'] )
	        else
	          consumer['status'] = 'crashed'
	          @redis.hset('decoupled.consumers', consumer['name'], consumer.to_json)
	        end
	      end
	    }
	  end
	  
	  def set_consumer_status(consumer)
	    consumer['status'] = 'running'
	    @redis.hset('decoupled.consumers', consumer['name'], consumer.to_json)
	  end
	  
	  def handle_channel_exception(channel, channel_close)
	    puts "Channel-level exception: code = #{channel_close.reply_code}, message = #{channel_close.reply_text}"
	  end
	end
end