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
    @logger = Logger.new(STDOUT)
    @logger.info("> Do register method: db: #{@database}.#{@collection}")

    prepareMeta()
  end # def register

  public
  def filter(event)
    begin
	# append fields
	@apDict['ALL'].each do |col,fields|
		data = @db[col].find(:_id => event['memid']).limit(1).first()
		unless data.nil?
			fields.each do |field|
				tmp = field.split(":")
				event[tmp[1]] = data[tmp[0]]
			end
			@logger.info("Info found!")
		else
			@logger.info("NIL: not found user info")
		end
	end

	# update field
	@upDict['ALL'].each do |col,uInfos|
		uHash = Hash.new()
		@logger.info(uInfos)
		uInfos.each do |info|
			tmp = info.split(":")
			field = tmp[0]
			method = tmp[1]
			val = tmp[2]
			if (method == 'inc')
				if (uHash.has_key?('$inc'))
					uHash['$inc'].push({field=>val})
				else
					uHash['$inc'] = [{field=>val}]
				end
			elsif method == 'set'
                                if (uHash.has_key?('$set'))
                                        uHash['$set'].push({field=>val})
                                else
                                        uHash['$set'] = [{field=>val}]
				end

			end
		end
		@logger.info(uHash)
		@db[col].update({"_id" => event['memid']}, uHash)
	end	

	
	#d = @db[@collection].find().limit(1).first()
    rescue Mongo::Error::NoServerAvailable => ex
	@logger.info("No server is available.")
	sleep @retry_delay
	@db = Mongo::Client.new(@uri).use(@database)
	retry
    end

    # filter_matched should go in the last line of our successful code
    filter_matched(event)
  end # def filter

  public
  def prepareMeta()
        @apDict = Hash.new()
	@upDict = Hash.new()	

	#apDict format:  {:ALL=>{"UserInfo"=>["level:lv", "session:s"]}}
	@apDict['ALL'] = Hash.new()
	
	#upDict format {"levelup"=>{"UserInfo"=>["level:inc:1.0"]}, "login"=>{"UserInfo"=>["session:inc:1.0"]}, "ALL"=>{"UserInfo"=>["step:inc:1.0"]}}
	

        @CDs = @db[@collection].find()
        @CDs.each do |doc|
                tmp = doc[:src].split(':')
		col = tmp[0]
                f = tmp[1]
		exF = doc[:exName]
                if (@apDict['ALL'].has_key?(col))
                        @apDict['ALL'][col].push("#{f}:#{exF}")
		else
			@apDict['ALL'][col] = ["#{f}:#{exF}"]
                end

		doc['updateActions'].each do |act,upInfo|
			if (@upDict.has_key?(act))
				if (@upDict[act].has_key?(col))	
					@upDict[act][col].push("#{f}:#{upInfo['formular']}:#{upInfo['val']}")
				else
					@upDict[act][col] = ["#{f}:#{upInfo['formular']}:#{upInfo['val']}"]
				end
			else
				@upDict[act] = Hash.new()
				@upDict[act][col] = ["#{f}:#{upInfo['formular']}:#{upInfo['val']}"]
			end
		end
        end
	@logger.info(@apDict)
	@logger.info(@upDict)
  end


end # class LogStash::Filters::Example
