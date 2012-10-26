# coding: utf-8
require "decoupled/version"
require 'json'
require 'java'
require File.dirname(__FILE__) + '/jars/commons-cli-1.1.jar'
require File.dirname(__FILE__) + '/jars/commons-io-1.2.jar'
require File.dirname(__FILE__) + '/jars/rabbitmq-client.jar'
require File.dirname(__FILE__) + '/jars/mongo-2.9.1.jar'
require 'redis'

java_import java.util.concurrent.Executors
# RabbitMQ imports
java_import com.rabbitmq.client.AMQP
java_import com.rabbitmq.client.Connection
java_import com.rabbitmq.client.ConnectionFactory
java_import com.rabbitmq.client.Channel
java_import com.rabbitmq.client.Consumer
java_import com.rabbitmq.client.DefaultConsumer
java_import java.util.concurrent.Executors
# MongoDB Imports
java_import com.mongodb.Mongo
java_import com.mongodb.MongoOptions
java_import com.mongodb.DB
java_import com.mongodb.DBCollection
java_import com.mongodb.BasicDBObject
java_import com.mongodb.DBObject
java_import com.mongodb.DBCursor
java_import org.bson.types.ObjectId
# RabbitMQ imports
java_import com.rabbitmq.client.Connection
java_import com.rabbitmq.client.Channel
# Your code goes here...

module Decoupled
  # Possible to use constants here

end

#require 'decoupled/util'
#require 'decoupled/worker'
#require 'decoupled/consumer'
require 'decoupled/scheduler'

options = Hash.new
options[:environment]      = "development"
#options[:logging]          = cnf["logging"].nil? ? "off" : cnf["logging"] == "" ? "off" : cnf["logging"]
options[:collections]      = ["schedules"]
options[:redis_host]       = "redisdb"
options[:amqp_host]        = "rabbitmq1"
options[:amqp_fallbacks]   = Array.new
options[:concurrent_count] = 2
options[:scheduler_name]   = "scheduler_test"
options[:scheduler_db]     = "skydb-development"

begin
    $0 = "starting decoupled:scheduler #{options[:scheduler_name]} with environment #{options[:environment]}"
  
    puts '+---------------------------------------------------------------------------------------------'
    puts "| starting Decoupled Scheduler (#{options[:scheduler_name]}) with the following parameters:"
    puts "|"
    if options[:config_file]
      puts "| configfile:       #{config_file}"
      puts "|"
    end
    puts "| environment:      #{options[:environment]}"
    puts "| custom collections:   #{options[:collections].empty? ? "" : options[:collections].join(",")}"

	@scheduler = Decoupled::Scheduler.new(options)
	@scheduler.start

	trap("TERM") { 
	    puts 'shutting down scheduler, can take some time...'
	    #loop = false
	    @scheduler.close_connections
	    exit 2
	}

	trap("SIGINT") {
	  puts 'shutting down scheduler, can take some time...'
	  #loop = false
	  @scheduler.close_connections
	  exit 2
	}

	trap("INT") { 
	  puts 'shutting down scheduler, can take some time...'
	  #loop = false
	  @scheduler.close_connections
	  exit 2
	}

rescue Exception => e 
	puts "Failure in Scheduler Test => #{e}"
  	puts "Closing all existing connections"
    @scheduler.close_connections
end