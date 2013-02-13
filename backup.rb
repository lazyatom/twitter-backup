#!/usr/bin/env ruby
require "bundler/setup"
require 'yajl'
require 'mongo'
require 'open-uri'

def debug(msg)
  puts msg if ENV["DEBUG"]
end

def tweet_collection(username)
  @db ||= Mongo::Connection.new("127.0.0.1").db("twitter")
  @tweet_collection ||= @db.collection(username)
end

def latest_stored_tweet_id(username)
  tweet = tweet_collection(username).find.sort(["id", "descending"]).limit(1).to_a[0]
  tweet && tweet["id"]
end

def load_tweets(username)
  url = "http://api.twitter.com/1/statuses/user_timeline.json?screen_name=#{username}&trim_user=1&count=200"
  url << "&since_id=#{latest_stored_tweet_id(username)}" if latest_stored_tweet_id(username)
  tweets = []
  result = nil
  page = 1
  until page > 1 && result.empty?
    debug "Fetching page #{page}..."
    open("#{url}&page=#{page}") do |f|
      page  += 1
      result = Yajl::Parser.parse(f.read)
      debug "got #{result.length} tweets"
      tweets.push *result if !result.empty?
    end
  end
  tweets
end

def store_tweets(username)
  tweets = load_tweets(username)
  puts "Importing #{tweets.size} new tweets..."
  tweets.reverse!
  tweets.each_with_index do |tweet, idx|
    puts tweet['id'] if (idx + 1) % 100 == 0
    tweet_collection(username).insert(tweet)
  end
end

store_tweets(ARGV[0]) if __FILE__ == $0
