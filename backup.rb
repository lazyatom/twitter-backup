#!/usr/bin/env ruby
require "bundler/setup"
require 'twitter'
require 'mongo'
require 'dotenv'

Dotenv.load

Twitter.configure do |config|
  config.consumer_key = ENV['CONSUMER_KEY']
  config.consumer_secret = ENV['CONSUMER_SECRET']
  config.oauth_token = ENV['OAUTH_TOKEN']
  config.oauth_token_secret = ENV['OAUTH_TOKEN_SECRET']
end

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
  tweets = []
  options = {trim_user: true, count: 200}
  options[:since_id] = latest_stored_tweet_id(username) if latest_stored_tweet_id(username)
  result = Twitter.user_timeline(username, options)
  tweets += result
  until result.empty?
    new_start = tweets.last.id - 1
    options[:max_id] = new_start
    result = Twitter.user_timeline(username, options)
    tweets += result
  end
  tweets.map(&:to_hash)
end

def store_tweets(username)
  tweets = load_tweets(username)
  puts "Importing #{tweets.size} new tweets..."
  tweets.reverse!
  tweets.each_with_index do |tweet, idx|
    puts tweet[:id] if (idx + 1) % 100 == 0
    begin
      tweet_collection(username).insert(tweet)
    rescue => e
      puts "failed to insert: #{tweet.inspect}"
      raise e
    end
  end
end

store_tweets(ARGV[0]) if __FILE__ == $0
