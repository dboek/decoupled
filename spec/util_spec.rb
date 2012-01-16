require 'spec_helper'

describe Decoupled::String do
  
  it "should underscore a String" do
    
    Decoupled::String.underscore("HelloWorld").should == "hello_world"
  end
  
end
