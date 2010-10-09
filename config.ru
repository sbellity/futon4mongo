$:.unshift(File.dirname(__FILE__))

require 'serve'
run Sinatra::Application

