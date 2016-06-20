# coding: utf-8

class Decoupled::Worker

  def initialize(count, conn, r_conn, payload)
    @count              = count
    @m                  = conn
    @r_conn             = r_conn
    @payload            = payload
  end

  def execute(job_klass)
    klass = Object.const_get(job_klass)
    jk = klass.new
    jk.execute_work( @payload, @m, @r_conn )

    @count.decrementAndGet
    puts ">>> DECREMENT CURRENT JOBS #{@count}"
  end

end
