# coding: utf-8
require "decoupled/version"
require 'json'
require 'java'
require File.dirname(__FILE__) + '/jars/commons-cli-1.1.jar'
require File.dirname(__FILE__) + '/jars/commons-io-1.2.jar'
require File.dirname(__FILE__) + '/jars/rabbitmq-client.jar'
#require File.dirname(__FILE__) + '/jars/mongo-2.9.1.jar'
#require File.dirname(__FILE__) + '/jars/mongo-2.11.3.jar'
require File.dirname(__FILE__) + '/jars/mongo-2.14.2.jar'

java_import java.util.concurrent.Executors
# RabbitMQ imports
java_import com.rabbitmq.client.AMQP
java_import com.rabbitmq.client.Connection
java_import com.rabbitmq.client.ConnectionFactory
java_import com.rabbitmq.client.Channel
java_import com.rabbitmq.client.Consumer
java_import com.rabbitmq.client.DefaultConsumer
#java_import java.util.concurrent.Executors
# MongoDB Imports
java_import com.mongodb.MongoClient
java_import com.mongodb.MongoClientOptions
java_import com.mongodb.DB
java_import com.mongodb.DBCollection
java_import com.mongodb.BasicDBObject
java_import com.mongodb.DBObject
java_import com.mongodb.DBCursor
java_import org.bson.types.ObjectId
# RabbitMQ imports
#java_import com.rabbitmq.client.Connection
#java_import com.rabbitmq.client.Channel
# Your code goes here...

module Decoupled
  # Possible to use constants here

end

require 'decoupled/util'
require 'decoupled/worker'
require 'decoupled/consumer'
#require 'decoupled/scheduler'
