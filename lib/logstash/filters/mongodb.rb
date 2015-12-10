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

  # Datafield to check action
  config :dataField, :validate => :string, :default => "action", :required => true

  # Debug
  config :debug, :validate => :boolean, :default => false

  public
  def register
    Mongo::Logger.logger = @logger
    conn = Mongo::Client.new(@uri)
    @db = conn.use(@database)
    
    if @debug 
      @logger = Logger.new(STDOUT)
    end

    @logger.info("> Do register method: db: #{@database}.#{@collection}")

    prepareMeta2()
  end # def register

  public
  def filter(event)
    begin
      @CDMeta.each do |col, info|
        _last = @db[col].find(:_id => event['memid']).limit(1).first()
        _current = _last.clone() if not(_last.nil?)

        @logger.info("CD info: #{info}")

        #Update for all action
        info[:update]['ALL'].each do |uInfo|
          tmp = uInfo.split(":")
          f = tmp[0]
          method = tmp[1].downcase
          val = event.sprintf(tmp[2])

          if "inc".eql?(method) && not(_last.nil?)
            _current[f] = _last[f].to_i + val.to_i
          elsif "setint".eql?(method) && not(_last.nil?)
            _current[f] = val.to_i
          elsif "setstr".eql?(method) && not(_last.nil?)
            _current[f] = val.to_s
          elsif "cre".eql?(method)
            @db[col].insert_one({"_id" => val, "createdAt" => DateTime.parse(event['@timestamp'].to_s)})
          end
        end

        #Update for specific action
        @logger.info("check key: #{event[@dataField]}")
        if info[:update].has_key?(event[@dataField])
          info[:update][event[@dataField]].each do |uInfo|
            tmp = uInfo.split(":")
            f = tmp[0]
            method = tmp[1].downcase
            val = event.sprintf(tmp[2])

            if "inc".eql?(method) && not(_last.nil?)
              @logger.info("> INC: #{f}")
              _current[f] = _last[f].to_i + val.to_i
            elsif "setint".eql?(method) && not(_last.nil?)
              _current[f] = val.to_i
            elsif "setstr".eql?(method) && not(_last.nil?)
              _current[f] = val.to_s
            elsif "cre".eql?(method)
              _last = {"_id" => val, "createdAt" => DateTime.parse(event['@timestamp'].to_s)}
              _current = _last.clone() 
              @db[col].insert_one(_current)
              @logger.info("> INSERT _last: #{_last}" )
              @logger.info("> INSERT _current: #{_current}" )
            end
          end
        end

        #Append for all action
        unless _last.nil?
          info[:append]['ALL'].each do |aInfo|
            tmp = aInfo.split(":")
            f = tmp[0]
            exF = tmp[1]
            code = tmp[2..-1].join(":")
            @logger.info("code: #{code}")
            if (code.length == 0)
              @logger.info(">> Normal set")
              event[exF] = _current[f]
            else
              begin
                @logger.info(">> eval code")
                event[exF] = eval(code)
              rescue Exception => ex
                @logger.info("EVALUATION CODE FAIL!")
              end
            end

          end
        end


        if not(_last.nil?) && not(_current.nil?)
          @logger.info("_current: #{_current}")
          @logger.info("_last: #{_last}")
          @db[col].update_one({"_id"=>event['memid']}, _current)
        end
      end
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
  def prepareMeta2()
    @CDMeta = Hash.new()

    @CDs = @db[@collection].find()
    @CDs.each do |doc|
      tmp = doc[:src].split(':')
      col = tmp[0]
      f = tmp[1]
      exF = doc[:exName]
      @logger.info("pre update Dict: #{@CDMeta}")
      #append process metadata
      unless "none".eql? doc[:scope].downcase
        if "user".eql? doc[:scope]
          if not @CDMeta.has_key?(col)
            @CDMeta[col] = Hash.new()
          end

          if not @CDMeta[col].has_key?(:append)
            @CDMeta[col][:append] = Hash.new()
          end

          if not @CDMeta[col][:append].has_key?('ALL')
            @CDMeta[col][:append]['ALL'] = Array.new()
          end

          @CDMeta[col][:append]['ALL'].push("#{f}:#{exF}:#{doc['appendCode']}")
        end
      end

      #update process metadata
      doc['updateActions'].each do |act,upInfo|
        if not @CDMeta.has_key?(col)
          @CDMeta[col] = Hash.new()
        end

        if not @CDMeta[col].has_key?(:update)
          @CDMeta[col][:update] = Hash.new()
        end

        if not @CDMeta[col][:update].has_key?(act)
          @CDMeta[col][:update][act] = Array.new()
        end

        @CDMeta[col][:update][act].push("#{f}:#{upInfo['formular']}:#{upInfo['val']}")
      end
    end

    @logger.info("update Dict: #{@CDMeta}")
  end # prepare data 2

  public
  def collectInfo(key, event)

  end

end # class LogStash::Filters::Example
