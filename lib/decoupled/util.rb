module Decoupled
  
  # http://api.rubyonrails.org/classes/ActiveSupport/Inflector.html#method-i-underscore
  class String
    def self.underscore(str)
      str.gsub(/::/, '/').
        gsub(/([A-Z]+)([A-Z][a-z])/,'\1_\2').
        gsub(/([a-z\d])([A-Z])/,'\1_\2').
        tr("-", "_").
        downcase
    end
  end
end