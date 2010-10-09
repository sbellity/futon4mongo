#!/usr/bin/env ruby 

# Licensed under the Apache License, Version 2.0 (the "License"); you may not use
# this file except in compliance with the License. You may obtain a copy of the
# License at
# 
#   http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software distributed
# under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
# CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.

require "rubygems"
require "sinatra"
require "net/http"
require "yajl"
require 'yajl/json_gem'
require "pp"
require "mongo"
require "uri"
require "cgi"

set :public, File.dirname(__FILE__)

before do
  content_type "application/json"
end

get "/" do
  { "mongodb" => "Welcome", "version" => "mongodb-1.0" }.to_json
end

get "/_config/*" do
  if params[:splat][0] == "query_servers/"
    # Dummy response for futon...
    '{"javascript":"/usr/local/bin/couchjs /usr/local/share/couchdb/server/main.js"}'
  end
end

# DB operations

get "/_all_dbs" do
  all_dbs.map { |n,c| c.map { |cn| [n,cn].join("/") } }.flatten.compact.to_json
end

get "/_uuids" do 
  referrer = URI.parse(request.env["HTTP_REFERER"])
  db_name, col_name = CGI::unescape(referrer.query).split("/", 2)

  if (db_name && col_name)
    { "uuids" => [BSON::ObjectID.new.to_s] }.to_json
  else 
    error_not_found
  end
end

get "/_active_tasks" do
  [].to_json
end

put "/:database/:collection/" do
  db.create_collection(params[:collection])
  ok 201
end

delete "/:database/:collection/" do
  db.drop_collection(col.name)
  ok
end

get "/:database/:collection/" do
  {
    "db_name" => [db.name, col.name].join("/"),
    "doc_count" => col.count,
    "disk_size" => 123
  }.to_json
end

post "/:database/:collection/_compact" do
  ok 202
end

get "/:database/:collection/_all_docs" do
  
  query = {}
  
  collection = col
  
  
  # results for pagination
  if startkey_docid
    puts "Getting _all_docs with startkey_docid : (class: #{startkey_docid.class})"
    query["_id"] = { "$gte" => startkey_docid }
  
  # Design documents are they are stored in the special (hidden) collection '__design'
  elsif startkey =~ /^_design.*/ && endkey =~ /^_design.*/
    collection = design_col
    query["collection"] = col.name
  
  elsif (!startkey.nil? || !endkey.nil?)
        
    # for docid direct access...
    if startkey && endkey && params[:limit] == "10" && endkey[-1] == 122
      s = BSON::ObjectID.from_string(startkey.ljust(24, "0")) rescue startkey
      e = BSON::ObjectID.from_string(endkey.gsub("z", "f").ljust(24, "f")) rescue endkey
      query["_id"] = {
        "$gte" => s,
        "$lte" => e
      }
    else
      query["_id"] = {}
      query["_id"]["$gte"] = startkey if startkey
      query["_id"]["$lte"] = endkey if endkey
    end
  end
  
  docs_list collection.find(query, query_options)
end

get "/:database/:collection/_design/:design_doc" do
  doc = design_col.find_one({ "_id" => "_design/#{params[:design_doc]}", "collection" => col.name })
  doc.to_json
end

delete "/:database/:collection/_design/:design_doc" do
  puts "Deleting design_doc... #{params[:design_doc]}"
  design_col.remove({"_id" => "_design/#{params[:design_doc]}", "collection" => col.name })
  ok
end

put "/:database/:collection/_design/:design_doc" do
  doc = Yajl::Parser.new.parse(request.body)
  doc["collection"] = col.name
  
  puts "\n\n\n\n--- Saving design doc _design/#{params[:design_doc]}"
  pp doc
  
  design_col.update({ "_id" => "_design/#{params[:design_doc]}" }, doc, :upsert => true)
  ok 201, { "id" => "_design/#{params[:design_doc]}", "rev" => "1-abc" }
end

post "/:database/:collection/_temp_view" do
  query_view Yajl::Parser.new.parse(request.body.read)
end

get "/:database/:collection/_design/:design_doc/_view/:view_name" do
  design_doc = design_col.find_one({ "_id" => "_design/#{params[:design_doc]}" })
  view = design_doc["views"][params[:view_name]] rescue nil
  
  query_view view
end


get "/:database/:collection/*" do
  doc = find_doc(params[:splat].join("/"))
  if doc.nil?
    error_not_found
  else
    doc["_id"] = doc["_id"].to_s
    doc.to_json
  end
end

put "/:database/:collection/*" do
  doc = Yajl::Parser.new.parse(request.body)
  
  original_doc = find_doc(params[:splat].join("/"))
  
  if original_doc.nil?
    id = col.insert(doc)
  else
    id = original_doc["_id"]
    doc["_id"] = id
    res = col.update({ "_id" => id }, doc)
  end
  
  ok 201, { "id" => id.to_s, "rev" => "1" }
end

delete "/:database/:collection/*" do
  doc = find_doc params[:splat].join("/")
  if doc.nil?
    error_not_found
  else
    col.remove({ "_id" => doc["_id"] })
    ok
  end
end

private

def ok st=200, options={}
  status st
  { "ok" => "true" }.merge(options).to_json
end

def error_not_found
  status 404
  { "error" => "not_found", "reason" => "missing" }.to_json
end

def db d=nil
  conn.db(d || params[:database]) rescue nil
end

def col c=nil, d=nil
  db(d).collection(c || params[:collection]) rescue nil
end

def design_col d=nil
  db(d).collection("__design")
end

def all_dbs include_system=false
  conn.database_names.inject({}) do |dbs, d|
    dbs.merge({ d => conn.db(d).collection_names.map { |c| c unless ((c.split(".")[0] == "system" && !include_system) || c == "__design") }.compact })
  end
end

def conn
  host = ARGV[0] || "127.0.0.1"
  port = ARGV[1] || 27017
  @conn ||= Mongo::Connection.new(host, port)
end

def startkey
  @startkey ||= getkey(params[:startkey])
end

def endkey
  @endkey ||= getkey(params[:endkey])
end

def startkey_docid
  @startkey_docid ||= getkey(params[:startkey_docid])
end

def getkey(key)
  puts "GetKey : #{key}"
  return nil if key.nil?
  key.gsub!('"', '')
  if key =~ /^[0-9]+$/
    return key.to_i
  else
    return BSON::ObjectID.from_string(key) rescue key
  end
end

def find_doc(id, collection=nil)
  (collection || col).find_one({ "_id" => getkey(id) }) || (collection || col).find_one({ "_id" => id.to_s })
end

def query_options
  options = {}
  options[:limit] = params[:limit].nil? ? 10 : params[:limit].to_i
  options[:sort] = [["_id", (params[:descending] == "true" ? 'descending' : 'ascending')]]
  options[:skip] = params[:skip].nil? ? 0 : params[:skip].to_i
  return options
end

def docs_list res, include_docs=false, include_docs_in_value=false
  results = res.to_a
  include_docs ||= ( params[:include_docs] == "true" )
  
  rows = results.map do |doc|
    d = { "id" => doc["_id"].to_s, "key" => doc["_id"].to_s }
    d["value"] = include_docs_in_value ? doc : doc.keys
    d["doc"] = doc if include_docs
    d
  end
  
  {
    "total_rows" => res.count,
    "offset" => query_options[:skip],
    "rows" => rows
   }.to_json
end

def query_view view_doc={}
  
  view_doc = Yajl::Parser.new.parse(view_doc) if view_doc.is_a? String
  
  puts "\n\n\n\ View doc"
  pp view_doc
  
  if view_doc["map"].nil?
    filter = view_doc
    options = {}
  else
    filter = Yajl::Parser.new.parse(view_doc["map"]) rescue {}
    options = Yajl::Parser.new.parse(view_doc["reduce"]) rescue {}
    reduce = options.delete("reduce")
    fields = options.delete("fields")
    keys = options.delete("keys")
    initial = options.delete("initial")
  end

  puts "\n\n\nFilter #{filter.class}: "
  pp filter
  puts "\n\n\nOptions #{options.class}: "
  pp options

  
  if params["group"] == "true" && reduce
    res = col.group(keys, filter, initial, reduce)
  else
    opts = query_options.merge({
      :fields => fields
    })
    res = col.find(filter, opts)
  end

  docs_list res, false, true
end
