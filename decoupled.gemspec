# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "decoupled/version"

Gem::Specification.new do |s|
  s.name        = "decoupled"
  s.version     = Decoupled::VERSION
  s.platform    = Gem::Platform::RUBY
  s.authors     = ["Daniel Boekhoff"]
  s.email       = ["daniel.boekhoff@unamana.com"]
  s.homepage    = %q{http://github.com/dboek/decoupled}
  s.summary     = %q{"Process and monitor Jobs, create and manage Schedules"}
  s.description = %q{""}

  s.rubyforge_project = "decoupled"

  s.files         = `git ls-files`.split("\n")
  s.test_files    = `git ls-files -- {test,spec,features}/*`.split("\n")
  s.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  #s.require_paths = ["lib"]
  
  #s.add_dependency(%q<amqp>, ["~> 0.8.0"])
  s.add_dependency(%q<redis>)
  s.add_dependency(%q<json>)
  s.add_dependency(%q<tzinfo>)
  
  s.add_development_dependency "rspec"
  s.add_development_dependency "ZenTest"
  s.add_development_dependency "spork"
  
  # s.add_runtime_dependency "rest-client"
end
