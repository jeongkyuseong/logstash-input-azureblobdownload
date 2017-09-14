# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"

require "azure"
require "base64"
require "securerandom"

# Reads events from Azure Blobs
class LogStash::Inputs::Azureblobdownload < LogStash::Inputs::Base
  # Define the plugin name
  config_name "azureblobdownload"

  # Codec
  # *Possible values available at https://www.elastic.co/guide/en/logstash/current/codec-plugins.html
  # *Most used: json_lines, line, etc.
  default :codec, "line"
  
  # storage_account_name
  # *Define the Azure Storage Account Name
  config :storage_account_name, :validate => :string, :required => true
  config :storagestate_account_name, :validate => :string, :required => true

  # storage_access_key
  # *Define the Azure Storage Access Key (available through the portal)
  config :storage_access_key, :validate => :string, :required => true
  config :storagestate_access_key, :validate => :string, :required => true

  # container
  # *Define the container to watch
  config :container, :validate => :string, :required => true

  # sleep_time
  # *Define the sleep_time between scanning for new data
  config :sleep_time, :validate => :number, :default => 10, :required => false
    
  # [New]
  # path_prefix
  # *Define the path prefix in the container in order to not take everything
  config :path_prefix, :validate => :array, :default => [""], :required => false

  # sincedb
  # *Define the Azure Storage Table where we can drop information about the the blob we're collecting. 
  # *Important! The sincedb will be on the container we're watching.
  # *By default, we don't use the sincedb but I recommend it if files gets updated.
  config :sincedb, :validate => :string, :required => true

  # ignore_older
  # When the file input discovers a file that was last modified
  # before the specified timespan in seconds, the file is ignored.
  # After it's discovery, if an ignored file is modified it is no
  # longer ignored and any new data is read. The default is 24 hours.
  config :ignore_older, :validate => :number, :default => 24 * 60 * 60, :required => false

  # Choose where Logstash starts initially reading blob: at the beginning or
  # at the end. The default behavior treats files like live streams and thus
  # starts at the end. If you have old data you want to import, set this
  # to 'beginning'.
  #
  # This option only modifies "first contact" situations where a file
  # is new and not seen before, i.e. files that don't have a current
  # position recorded in a sincedb file read by Logstash. If a file
  # has already been seen before, this option has no effect and the
  # position recorded in the sincedb file will be used.
  config :start_position, :validate => [ "beginning", "end"], :default => "end", :required => false




  # Initialize the plugin
  def initialize(*args)
    @logger.info("initialize()................START!")
    super(*args)
  end # def initialize
  



  public
  def register
    Azure.configure do |config|
      config.storage_account_name = @storage_account_name
      config.storage_access_key = @storage_access_key
      config.storage_table_host = "https://#{@storage_account_name}.table.core.windows.net"
    end
    @azure_blob = Azure::Blob::BlobService.new
    
    Azure.configure do |config|
      config.storage_account_name = @storagestate_account_name
      config.storage_access_key = @storagestate_access_key
      config.storage_table_host = "https://#{@storagestate_account_name}.table.core.windows.net"
    end    
    @azure_table = Azure::Table::TableService.new
    init_wad_table

  end # def register



  
  # Initialize the WAD Table if we have a sincedb defined.
  def init_wad_table
    begin
      @azure_table.create_table(@sincedb) # Be sure that the table name is properly named.
    rescue
      @logger.info("[#{storage_account_name}] init_wad_table: Table #{@sincedb} already exists.")
    end
  end # def init_wad_table




  # List the blob names in the container. If we have any path pattern defined, it will filter 
  # the blob names from the list. The status of the blobs will be persisted in the WAD table.
  #
  # Returns the list of blob_names to read from.
  def list_blobs
    blobs = Hash.new
    now_time = DateTime.now.new_offset(0)

    @logger.info("list_blobs: Looking for blobs in #{path_prefix.length} paths")

    # Mutate the prefix list if it contains RANGE placeholder
    path_prefix_new = Array.new
    path_prefix.each do |prefix|
       if prefix.include? "$RANGE"
          rangevars = prefix.match(/.*\$RANGE_(\d+)_TO_(\d+).*/).to_a
          (rangevars[1]..rangevars[2]).each do |n|
             path_prefix_new.push prefix.gsub(/\$RANGE_\d+_TO_\d+\$/,n)
          end
       else
          path_prefix_new.push prefix
       end
    end

    path_prefix_new.each do |prefix|
        loop do
           # TODO: Extract placeholder for data prefix
           prefix = prefix.gsub("$DATE$",now_time.strftime("%y%m%d"))
           continuation_token = NIL
           entries = @azure_blob.list_blobs(@container, { :timeout => 10, :marker => continuation_token, :prefix => prefix})
           entries.each do |entry|
               entry_last_modified = DateTime.parse(entry.properties[:last_modified])
               elapsed_seconds = ((now_time - entry_last_modified) * 24 * 60 * 60).to_i
               if (elapsed_seconds <= @ignore_older)
                   blobs[entry.name] = entry
               end
           end

           continuation_token = entries.continuation_token
           break if continuation_token.empty?
        end
    end

    @logger.info("list_blobs: Finished looking for blobs. #{blobs.length} are queued for possible candidate with new data")

    return blobs
  end # def list_blobs



  # Retrieve all sincedb rows with continuation style queries
  def list_sinceDbContainerEntities
    entities = Set.new
    loop do
      continuation_token = NIL
      entries = @azure_table.query_entities(@sincedb, { :filter => "PartitionKey eq '#{container}'", :continuation_token => continuation_token}) 
      entries.each do |entry|
          entities << entry
      end
      continuation_token = entries.continuation_token
      break if continuation_token.nil? or continuation_token.empty?
      @logger.warn("[#{storage_account_name}] list_sinceDbContainerEntities: Continuation token utilized!")
    end
    return entities
  end # def list_sinceDbContainerEntities




  # Process the plugin ans start watching.
  def process(output_queue)
    # Get the blobs that are matching the prefix declared
    blobs = list_blobs

    # If there is a sincedb table
    if (@sincedb)
      existing_entities = list_sinceDbContainerEntities

      # For each blob that is validated for processing (due to prefix)
      blobs.each do |blob_name, blob_info|
        @logger.info("[#{storage_account_name}] Blob attempt on: [#{blob_info.name}]")
        # Extract its path
        basepath = blob_info.name.split(File::SEPARATOR)[0]

        # Find in the sincedb entries and init the ENTITY STATE
        blob_name_encoded = Base64.strict_encode64(blob_info.name)
        entityIndex = existing_entities.find_index {|entity| entity.properties["RowKey"] == blob_name_encoded }
        entity = { 
          "PartitionKey" => @container, 
          "RowKey" => blob_name_encoded, 
          "ByteOffset" => 0, # First contact, start_position is beginning by default
          "ETag" => NIL,
          "BlobName" => blob_info.name
        }

        # If we have a match in the sincedb, update the ENTITY STATE with this info
        if (entityIndex)
           foundEntity = existing_entities.to_a[entityIndex];
           entity["ByteOffset"] = foundEntity.properties["ByteOffset"]
           entity["ETag"] = foundEntity.properties["ETag"] 
           @logger.info("[#{storage_account_name} - #{basepath}] Blob is now #{blob_info.properties[:etag]}:#{blob_info.properties[:content_length]} - sincedb is: #{entity["ETag"]}:#{entity["ByteOffset"]}")
        # If not then it means that either we just started (logstash) or new rotated file created
        elsif (@start_position === "end")
           @logger.info("[#{storage_account_name} - #{basepath}] Blob not in sincedb (mode #{@start_position}) so: Starting at #{blob_info.properties[:content_length]}")
           entity["ByteOffset"] = blob_info.properties[:content_length]
           entity["ETag"] = blob_info.properties[:etag]
           @azure_table.insert_or_merge_entity(@sincedb, entity)
        elsif (@start_position === "beginning")
           @logger.info("[#{storage_account_name} - #{basepath}] Blob not in sincedb (mode #{@start_position}) so: Starting at 0")
           entity["ByteOffset"] = 0
           entity["ETag"] = ""
           @azure_table.insert_or_merge_entity(@sincedb, entity)
        end

        # If blob has changed, read it
        if entity["ByteOffset"] < blob_info.properties[:content_length]
           @logger.info("[#{storage_account_name} - #{basepath}] Blob processing started")
           count = 0
           blob, content = @azure_blob.get_blob(@container,  blob_info.name, { :start_range => entity["ByteOffset"], :end_range =>  blob_info.properties[:content_length] })
           @codec.decode(content) do |event|
               decorate(event)
               event.set("file", blob_info.name)
               output_queue << event
               count = count + 1
           end 
           @logger.info("[#{storage_account_name} - #{basepath}] Submitting #{count} events [#{blob_info.name}]")

           # Update sincedb with the latest informations we used while processing the blob.
           entity["ByteOffset"] = blob_info.properties[:content_length]
           entity["ETag"] = blob_info.properties[:etag]
           @azure_table.insert_or_merge_entity(@sincedb, entity)
        else
           @logger.info("[#{storage_account_name} - #{basepath}] Blob already up to date")
        end
        
      end # For each blob
    end # if sincedb
    
    # Shutdown signal for graceful shutdown in LogStash
    rescue LogStash::ShutdownSignal => e
      raise e
    rescue => e
      @logger.error("#{DateTime.now} Oh My, An error occurred.", :exception => e)
  end # def process



  
  # Run the plugin (Called directly by LogStash)
  # This is just a loop with a small sleep
  public
  def run(output_queue)
	@logger.info("run()...............START!") 
	# Infinite processing loop.
    while !stop?
      process(output_queue)
      # After the first processing set position to start to 
      #  be consistent with rotations
      @start_position = "beginning"      
      sleep sleep_time
    end # loop
  end # def run



 
  public
  def teardown
    # Nothing to do.
    @logger.info("Teardown")
  end # def teardown



end # class LogStash::Inputs::Azureblobmulti
