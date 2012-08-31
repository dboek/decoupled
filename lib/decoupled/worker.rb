# coding: utf-8

class Decoupled::Worker

  def initialize(count, conn, payload)
    @count              = count
    @m                  = conn
    @payload            = payload
  end

  def execute(job_klass)
    klass = Object.const_get(job_klass)
    jk = klass.new
    jk.execute_work( @payload, @m )

    @count.decrementAndGet
  end

end