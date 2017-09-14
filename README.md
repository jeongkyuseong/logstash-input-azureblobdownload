# Logstash input plugin for Azure Storage Blobs

## Summary
This plugin reads and parses data from Azure Storage Blobs.

## Installation
You can install this plugin using the Logstash "plugin" or "logstash-plugin" (for newer versions of Logstash) command:
```sh
logstash-plugin install logstash-input-azureblob
```

For more information, see Logstash reference [Working with plugins](https://www.elastic.co/guide/en/logstash/current/working-with-plugins.html).

## Configuration
### Required Parameters
__*storage_account_name*__

The Azure storage account name.

__*storage_access_key*__

The access key to the storage account.

__*container*__

The blob container name.

### Optional Parameters
__*codec*__

The codec used to decode the blob. By default *json_lines* is selected. For normal log file, use *line* or other existing codec.

* **Default value:** *json_lines*

__*sleep_time*__

The sleep time before scanning for new data. 

* **Default value:** *10* seconds.
* **Note:** Does not seems to be implemented

__*sincedb*__

The Azure Table name to keep track of what have been done like when we 
use the file plugin. This define the table name that will be used during 
the process. **IMPORTANT** This will __not__ take into account any *.lock* 
files. It will also __not__ create any *.lock* files. 

* **Default value:** No default value, if a value is defined, than it will 
create the *sincedb* table in the blob account.

__*ignore_older*__

When the file input discovers a file that was last modified before the 
specified timespan in seconds, the file is ignored. After it's discovery, 
if an ignored file is modified it is no longer ignored and any new data 
is read. The default is 24 hours.

* **Default value:** *24&#42;60&#42;60* (24h)

__*start_position*__

Choose where Logstash starts initially reading blob: at the beginning or
at the end. The default behavior treats files like live streams and thus
starts at the end. If you have old data you want to import, set this
to 'beginning'.

This option only modifies *"first contact"* situations where a file
is new and not seen before, **i.e.** files that don't have a current
position recorded in a sincedb read by Logstash. If a file
has already been seen before, this option has no effect and the
position recorded in the sincedb file will be used.

* **Possible values:** &#91;beginning &#124; end&#93;
* **Dependency:** *sincedb* needs to be activated. 
* **Default value:** *beginning*

__*path_prefix*__

Array of blob "path" prefixes. It defines the path prefix to watch in the 
blob container. Path are defined by the blob name (i.e.: &#91;"path/to/blob.log"&#93;). 
Regex cannot really be used to optimize perfs.

I recommend to use the paths in order to speed up the processing. *By example, 
WebApp on azure with IIS logging enabled will create one folder per hour. If 
you keep the logs for a long retention it will select all before keeping only 
the last modified ones.*

* **Default value:** *&#91;&quot;&quot;&#93;*

***

### Example 1 Basic (out of the box)
Read from a blob (any type) and send it to ElasticSearch.
```
input
{
    azureblob
    {
        storage_account_name => "mystorageaccount"
        storage_access_key => "VGhpcyBpcyBhIGZha2Uga2V5Lg=="
        container => "mycontainer"
    }
}
output
{
  elasticsearch {
    hosts => "localhost"
    index => "logstash-azureblob-%{+YYYY-MM-dd}"
  }
} 
```

#### What will it do
It will get the blob, create an empty lock file (512 bytes) and read the entire blob **only once**. Each iteration of the plugin will take a new file and create a new lock file and push the original file to ElasticSearch. If any modification are made on the blob file, it won't be taken into account to push the new data to ElasticSearch. (*No use of sincedb in this situation*)

### Example 2 Advanced (Using sincedb and some other features)
Read from a blob and send it to ElasticSearch and keep track of where we are in the file.

```
input
{
    azureblob
    {
        storage_account_name => "mystorageaccount"
        storage_access_key   => "VGhpcyBpcyBhIGZha2Uga2V5Lg=="
        container            => "mycontainer"
        codec                => "line"         # Optional override => use line instead of json
        sincedb              => "sincedb"      # Optional          => Activate the sincedb in Azure table
        sleep_time           => 60             # Optional override => Azure IIS blob are updated each minutes
        ignore_older         => 2*60*60        # Optional override => Set to 2hours instead of 24 hours
        path_prefix          => ["ServerA/iis/2016/06/30/", "ServerA/log4net/"]
        start_position       => "end"          # Optional override => First contact set the sincedb to the end
    }
}
output
{
  elasticsearch {
    hosts => "localhost"
    index => "logstash-azureblob-%{+YYYY-MM-dd}"
  }
}
```

## More information
The source code of this plugin is hosted in GitHub repo [Microsoft Azure Diagnostics with ELK](https://github.com/Azure/azure-diagnostics-tools). We welcome you to provide feedback and/or contribute to the project.