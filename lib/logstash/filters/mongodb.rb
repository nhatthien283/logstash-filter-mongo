# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"
require "mongo"

# This example filter will replace the contents of the default 
# message field with whatever you specify in the configuration.
#
# It is only intended to be used as an example.
class LogStash::Filters::Mongodb < LogStash::Filters::Base

  # Setting the config_name here is required. This is how you
  # configure this filter from your Logstash config.
  #
  # filter {
  #   example {
  #     message => "My message..."
  #   }
  # }
  #
  config_name "mongodb"
  
  config :uri, :validate => :string, :required => true

   # The database to use
  config :database, :validate => :string, :required => true

  # The collection to use. This value can use `%{foo}` values to dynamically
  # select a collection based on data in the event.
  config :collection, :validate => :string, :required => true

  # Number of seconds to wait after failure before retrying
  config :retry_delay, :validate => :number, :default => 3, :required => false

  # Replace the message with this value.
  config :message, :validate => :string, :default => "Hello World!"
  

  public
  def register
    Mongo::Logger.logger = @logger
    conn = Mongo::Client.new(@uri)
    @db = conn.use(@database)
  end # def register

  public
  def filter(event)

    if @message
      # Replace the event message with our message as configured in the
      # config file.
#      event["message"] = @message
	event["message"] = "thien thien thien"
    end

    # filter_matched should go in the last line of our successful code
    filter_matched(event)
  end # def filter
end # class LogStash::Filters::Example
