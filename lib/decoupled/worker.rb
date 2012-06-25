# coding: utf-8
require 'nokogiri'
require 'mechanize'

class Decoupled::Worker

  def initialize(count, conn, payload)
    @count    = count
    @m        = conn
    @payload  = payload
  end

  def execute(job_klass)
    klass = Object.const_get(job_klass)
    jk = klass.new
    jk.execute_work( @payload )

    @count.decrementAndGet
  end

  #def execute
  #  begin
  #    agent = Mechanize.new
  #    agent.max_history = 1
  #    puts @payload
  #    page = agent.get('http://www.' + @payload)
  #    page.body.force_encoding('utf-8')
  #    doc = Nokogiri::HTML( page.body )
#
  #    puts page.title.force_encoding('utf-8')
  #  rescue => e
  #    puts e.inspect
  #  end
#
  #  worked_for = rand(5)
  #  sleep( worked_for )
  #  
  #  @count.decrementAndGet
  #end

  def mongo_test_calls
    doc = BasicDBObject.new

    doc.put('name', 'Daniel')

    addr = BasicDBObject.new
    addr.put('street', 'Lindenstr.')
    addr.put('city', 'Lüneburg')

    doc.put('address', addr)

    db = @m.getDB( "jruby_tests" )
    db.requestStart()

    coll = db.getCollection('names')
    coll.insert(doc)

    db.requestDone()
  end

end