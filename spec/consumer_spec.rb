require 'spec_helper'
require 'evented-spec'

describe Decoupled::Consumer do
  include EventedSpec::AMQPSpec
  default_options :host => '192.168.1.111'
  
  it "should be initializable" do
    consumer_name = 'consumer'
    channel = AMQP::Channel.new
    queue_name = 'search'
    jobs_per_consumer = 1
    process_class = "JobClass"
    
    consumer = Decoupled::Consumer.new(consumer_name, channel, queue_name, jobs_per_consumer, process_class)
    
    consumer.should != nil
  end
  
end
