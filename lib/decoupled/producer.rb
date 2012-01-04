# coding: utf-8
module Decoupled
	class Producer
	  
	  def initialize(channel, exchange)
	    @channel = channel
	    @exchange = exchange
	  end
	
	  def publish(message, options = {})
	    @exchange.publish(message, options)
	  end
	
	  def handle_channel_exception(channel, channel_close)
	    puts "Producer Channel-level exception: code = #{channel_close.reply_code}, message = #{channel_close.reply_text}"
	  end
	end
end