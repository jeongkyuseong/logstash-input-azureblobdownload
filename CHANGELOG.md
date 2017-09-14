## 2017.01.03 (Dimitris Strevinas)
* Modified to expand ranges of blob locations
* Sincedb location can be send to other storage account
* Better logging
* Various fixes is concurrency

## 2016.07.01
* Updated the *README.md*
* Implemented *sleep_time*
* Added *sincedb* parameter (Use Azure table)
* Added *ignore_older* parameter (work as file plugin)
* Added *start_position* parameter (work as file plugin)
* Added *path_prefix* parameter (no wildcard accepted, using Azure blob API)
* Added some logs for debugging

### Not yet completed
* Removed the milestone (deprecated)

## 2016.05.05
* Made the plugin to respect Logstash shutdown signal.
* Updated the *logstash-core* runtime dependency requirement to '~> 2.0'.
* Updated the *logstash-devutils* development dependency requirement to '>= 0.0.16'
