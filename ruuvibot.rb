#!/usr/bin/env ruby
require 'rubygems'
require 'mqtt'
require 'work_queue'
require 'json'

MOSQUITTO_HOST = "localhost"
INFLUX_URL = "http://localhost:8086/write?db=heishamon"
PATTERN = ARGV[0] || "ruuvi/#"

def log(message)
  STDOUT.puts "#{Time.now} #{message}"
  STDOUT.flush
end

def temperature(data)
  (Integer(data[16, 4], 16) * 0.005).round(2)
end

def humidity(data)
  (Integer(data[20, 4], 16) * 0.0025).round(2)
end

# background job queue with multiple workers
queue = WorkQueue.new(5, nil)

# keep reconnecting
while true do
  begin
  # connect to local mosquitto instance
    MQTT::Client.connect(MOSQUITTO_HOST) do |mqtt|
      log "connected to mosquitto on #{MOSQUITTO_HOST}"

      # subscribe to all ruuvi topics
      log "subscribing to #{PATTERN}"
      mqtt.get(PATTERN) do |topic, message|

        sensor = topic[-5,5]
        data = JSON.parse(message)["data"]

        # store temperature in influxdb
        queue.enqueue_b do 
          temperature = temperature(data)
          log "#{ruuvi/#{sensor}/temperature}: #{temperature}"
          system "curl -XPOST '#{INFLUX_URL}' --data-binary 'ruuvi/#{sensor}/temperature value=#{temperature}'"
        end

        # store humidity in influxdb
        queue.enqueue_b do 
          humidity = humidity(data)
          log "#{ruuvi/#{sensor}/humidity}: #{humidity}"
          system "curl -XPOST '#{INFLUX_URL}' --data-binary 'ruuvi/#{sensor}/humidity value=#{humidity}'"
        end
      end
    end
  rescue Errno::ECONNREFUSED
    log "connecting to mosquitto on #{MOSQUITTO_HOST} ..."
    sleep(5)
  end
end