A r2dfly.json Template for the Redis Dragonfly workflow should be created and made available as baseline file for import into the running NiFi system.

When the user first logs in to the NiFi server the r2dfly flow should already be loaded and only the properties that define the target and source should need adjusting to enable transfer of the data from source to target.

As much as possible, the setup of the Redis-dragonfly migration should be managed using the NiFi CLI

[nifi-command-line](https://nifi.apache.org/docs/nifi-docs/html/toolkit-guide.html#nifi_CLI)
 
