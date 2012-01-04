# coding: utf-8
module Decoupled
  class Scheduler
  
    def initialize(channel, queue_name) 
      @increment = 0
      @schedules = Hash.new
      @redis = Redis.new
      @queue_name = queue_name

      @channel = channel
      @channel.on_error(&method(:handle_channel_exception))
    
      # Start a cleanup process, delete all old timers in the redis hashlist
      # del old schedules, should be restarted in next version
      @redis.del('decoupled.schedules')
      # Old schedules should be started again.
    end

    def start
      @queue = @channel.queue(@queue_name, :exclusive => false)
      @queue.subscribe(:ack => true, &self.method(:incoming_schedule))
    
      # read schedule from file
      # check in redis for schedules added by webinterface
    end
  
    #  Possible formats for sending a schedule request
    #  { send_in: 180, send_to_queue: livesearch, payload: }
    #  { send_at: 578476520465, send_to_queue: livesearch, payload: }
    #  { send_every: 20, send_to_queue: livesearch, payload: }
    #  
    #  { schedule_id: 201108211, action: [delete,update] }
    def incoming_schedule(metadata, message)
      message_hash = JSON.parse(message)
    
      if message_hash['schedule_id']
        handle_schedule_update(message_hash)
        @channel.acknowledge(metadata.delivery_tag, false)
      else
        handle_new_schedule(message_hash)
        @channel.acknowledge(metadata.delivery_tag, false)
      end
    end
  
    def handle_new_schedule(message_hash)
      if message_hash['send_at']
        puts "scheduled a send_at message"
        seconds = message_hash['send_at'] - Time.now.to_i
        timer = start_once_timer(seconds, message_hash['send_to_queue'], message_hash['payload'])
        schedule_id = create_id
        @schedules[schedule_id] = timer
        # Save payload to Redis Hash
        persist_schedule_details(schedule_id, message_hash)
      
      elsif message_hash['send_in']
        puts "scheduled a send_in message"
        start_once_timer( message_hash['send_in'], message_hash['send_to_queue'], message_hash['payload'] ) 
      
      elsif message_hash['send_every']
        puts "scheduled a send_every message"
        timer = start_periodic_timer( message_hash['send_every'], message_hash['send_to_queue'], message_hash['payload'] )
        schedule_id = create_id
        @schedules[schedule_id] = timer
        # Save payload to Redis Hash
        persist_schedule_details(schedule_id, message_hash) 
      
        # Additional start_at a given time, and repeat for a given timespan,
        # like start at the full hour and repeat every 60 Minutes
        # TODO -- plus user should be able to send readable 00:05:00
      else
        puts "message incomplete or malformed"
      end
    end
  
    def handle_schedule_update(message_hash)
      # or stopping schedules, request coming from the webinterface
      # or adding new schedules
      timer = @schedules[message_hash['schedule_id']]
    
      if message_hash['action'] == 'delete'
        if timer
          timer.cancel
          @schedules.delete(message_hash['schedule_id'])
          @redis.hdel('decoupled.schedules', message_hash['schedule_id'])
          puts 'schedule deleted '+message_hash['schedule_id']
        else
          puts "schedule_id does not exist #{message_hash['schedule_id']}"
        end
      end
    end
  
    # Should return a timer object to be able to stop the timer from the webinterface
    # 
    def start_once_timer(send_in, queue, payload)
      # Timer once, starts after x seconds. cannot be stopped
      timer = EventMachine::Timer.new(send_in) do
        # Send the payload to the given message queue
        exchange = @channel.direct('')
        exchange.publish payload.to_json, :routing_key => queue
      end
      timer
    end
  
    # Should return a timer object to be able to stop the timer from the webinterface
    # 
    def start_periodic_timer(every, queue, payload)
      timer = EventMachine::PeriodicTimer.new(every) do
        # Send the payload to the given message queue
        exchange = @channel.direct('')
        exchange.publish payload.to_json, :routing_key => queue
      end
      timer
    end
  
    def handle_channel_exception(channel, channel_close)
      puts "Channel-level exception: code = #{channel_close.reply_code}, message = #{channel_close.reply_text}"
    end
  
    def create_id
      @increment += 1
    
      Time.now.strftime("%Y%m%d") + @increment.to_s
    end
  
    def persist_schedule_details(schedule_id, message_hash) 
      @redis.hset('decoupled.schedules', schedule_id, message_hash.to_json)
    end
  
    def get_schedule(schedule_id)
      @redis.hget('decoupled.schedule', schedule_id)
    end
  
  
  end
end