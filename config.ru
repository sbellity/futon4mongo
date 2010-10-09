$:.unshift(File.dirname(__FILE__))

require 'rubygems'
require 'bundler'
Bundler.require(:default)

require 'serve'
run Sinatra::Application

