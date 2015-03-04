camps-crawler
=====

A Python crawler for distributed (client-server) data collection. The code to do the actual crawling is written by the user and the script takes care of all management needed to distribute resources to be collected. 

### Dependencies

This program was written in Python 2.7 and tested under Linux and Windows. It depends on the following external Python packages:

* [xmltodict](https://github.com/martinblech/xmltodict)
* [MySQL Connector/Python](http://dev.mysql.com/downloads/connector/python/)

Quick demo
-----

To see the crawler in action, open a terminal window an go into the /demo directory. Then, type the following command:

`python ../server.py config.demo.xml`

This will initialiaze the server, setting it to distribute the resources stored in the file `resources.csv`. Open another terminal window, go into the /demo directory and type:

`python ../manager.py config.demo.xml -s extended`

The manager will show you extended status information about the data collection process, as informed by the server. Under *Global Info* section, note that there are 10 resources available to be crawled and 1 already crawled. Type the following command:

`python ../client.py config.demo.xml`

This will initialize a client. The client is configured to use the crawling code of the *DemoCrawler* class, wich can be found in the file `crawler.py`. The code just receives a resource ID, waits for some time and then returns some information related to the resource to the server. 

Besides that, two filters are used to save new resource IDs returned by the client. These new IDs are stored in the files `new_resources.csv` and `new_resources.json`. In the case of the JSON file, a *RolloverFilePersistenceHandler* is used, configured to save a maximum of 5 resources per file. This way, at the end of demo execution you should see 3 JSON files in the folder: `new_resources.json`, `new_resources.json.1` and `new_resources.json.2`. The last two files are automatically created by the rollover handler.


Quick setup
-----

To setup the collection program you basically have to write a crawler class with an implementation of the crawl method that suits your scenario (inside file `crawler.py`) and adjust the settings inside the XML configuration file. Assuming the simplest configuration possible, the setup workflow would be as follows:

1. Implement the crawl method inside `crawler.py`
2. Create a XML configuration file, for example `config.xml`
3. For global settings specify:
    * Hostname of the server's machine
    * Port where server will be listening for new connections
4. For server settings specify the persistence handler to be used (as well as the handler particular options)
5. For client settings specify the name of the crawler class
6. On server's machine, run the command `python ./server.py config.xml`
7. On each client machine, start one or more clients by running the command `python ./client.py config.xml`
8. Monitor and manage the collection process using the script `manager.py`
9. Wait until the collection is finished
10. Enjoy your new data!

The final `config.xml` would be something like this:

```xml
<?xml version="1.0" encoding="ISO8859-1" ?>
<config>
    <global>
        <connection>
            <address>myserver</address>
            <port>7777</port>
        </connection>
    </global>
    <server>
        <persistence>
            <handler>
                <!--- Handler specific configurations--->
            </handler>
        </persistence>
    </server>
    <client>
        <crawler>
            <class>MyCrawler</class>
        </crawler>
    </client>
</config>
```


Architecture overview
-----

The data collection code is built as a simple client-server program. Every resource that needs to be collected is identified in the program by an ID. For example, the resources could be web pages, wich are identified by their URL, or they could be users in a social network, wich are identified by their user ID code. The server side is responsible for distribute resource IDs among clients and maintain the collection status of every resource. The clients are responsible for calling the code written by the user to do the actual crawling of the resources sent by the server. 

To allow extensions and customizations, the server side of the program was designed in layers. 

At the top level is the request handling code (`serverlib.py`). The server starts a new thread for each new client, encapsulating a loop where all transactions between server and client are dealt with. Managment operations (like remove clients and shut down server) are also performed by this code in response to requests made by the manager (`manager.py`).

The next layer is the filters layer. Filters (`filters.py`) are segments of code that can be executed before a resource is sent to a client and/or after the resource have been crawled. Filters are not essential to the regular work of the server. They were implemented as way to easily allow pre and post-processing of resources when the scenario requires it.

Finally, at the bottom level is the persistence layer (`persistence.py`), whose main purpose is to load and save information about the resources beign processed. This layer provides a common interface to persistence operations, freeing the server of the necessity to know if the resources are beign stored in a file or in a database and how to deal with the particular details of each persistence alternative. The persistence layer can also be used with filters (in fact, it could even be used in the crawler, at the client side, if the user wants to).

To give an idea on how these things go togheter, one request round of the program is described bellow:

1. Server starts
2. Client starts
3. Server receives client request for connection. It starts a new thread to handle requests for this new client
4. Client requests a new resource ID to crawl
5. Server talks with persistence layer to obtain a resouce ID not yet crawled
6. Having a resource ID to send, server first applies pre-processing filters (if there are any filters to apply)
7. Server sends the resource ID and filters results to client
8. Client calls user code to do the actual crawling of the resource
9. Client sends the results of the crawling process to server
10. Server calls back post-processing filters (if there are any filters to call back)
11. Server talks with persistence layer to save the status of the crawling process for that resource
12. Back to step 4


Configuration options
-----

All server and client configurations are made in a XML configuration file. Logging and verbose options are also available through the command line and override the global settings specified in the configuration file. Type `python .\server.py -h` or `python .\client.py -h` to see more information about command line options.

###General configuration options

The example bellow shows a complete configuration file, with all options available. For each option, it also shows its value type, wether the option is required or not and what is the default value. Boolean options accepts the following values for True: true, t, yes, y, on, 1; and the following values for False: false, f, no, n, off, 0. In both cases, the values are not case sensitive.

```xml
<?xml version="1.0" encoding="ISO8859-1" ?>
<config>
    <global>
        <connection>
            <address>String - Required</address>
            <port>Integer - Required</port>
        </connection>
        <feedback>Boolean - Optional (Default: False)</feedback>
        <echo>
            <!--- The echo section itself is optional --->
            <verbose>Boolean - Optional (Default: None)</verbose>
            <logging>Boolean - Optional (Default: None)</logging>
            <loggingpath>String - Optional (Default: None)</loggingpath>
        </echo>
    </global>
    <server>
        <echo>
            <!--- The echo section itself is optional --->
            <verbose>Optional (Default: False)</verbose>
            <logging>Optional (Default: True)</logging>
            <loggingpath>Optional (Default: ".")</loggingpath>
        </echo>
        <loopforever>Optional (Default: False)</loopforever>
        <persistence>
            <handler>
                <class>Required</class>
                <echo>
                    <!--- The echo section itself is optional --->
                    <verbose>Optional (Default: False)</verbose>
                    <logging>Optional (Default: True)</logging>
                    <loggingpath>Optional (Default: ".")</loggingpath>
                </echo>
                <!--- Other handler specific configurations--->
            </handler>
        </persistence>
        <filtering>
            <!--- The filtering section itself is optional --->
            <filter>
                <class>Required</class>
                <name>Optional (Default: class name)</name>
                <parallel>Optional (Default: False)</parallel>
                <echo>
                    <!--- The echo section itself is optional --->
                    <verbose>Optional (Default: False)</verbose>
                    <logging>Optional (Default: True)</logging>
                    <loggingpath>Optional (Default: ".")</loggingpath>
                </echo>
                <!--- Other filter specific configurations --->
            </filter>
        </filtering>
    </server>
    <client>
        <echo>
            <!--- The echo section itself is optional --->
            <verbose>Optional (Default: False)</verbose>
            <logging>Optional (Default: True)</logging>
            <loggingpath>Optional (Default: ".")</loggingpath>
        </echo>
        <crawler>
            <class>Required</class>
            <echo>
                <!--- The echo section itself is optional --->
                <verbose>Optional (Default: False)</verbose>
                <logging>Optional (Default: True)</logging>
                <loggingpath>Optional (Default: ".")</loggingpath>
            </echo>
        </crawler>
    </client>
</config>
```

General options explanation follows:


1. `global`
    * `connection`
        * `address`: server's machine hostname.
        * `port`: port where server will be listening for new connections.
    * `feedback`: enable/disable saving of new resources sent to server. When clients send new resources to server and feedback is enabled, these resources are saved in the same file where the original resources are stored and then become part of the list of resources to be crawled. So, this could be used to do a snowball crawling, for example.
    * `echo`: echo configurations placed here override any echo configurations in other sections of the configuration file. This can only be overriden by command line options.
        * `verbose`: enable/disable information messages on screen.
        * `logging`: enable/disable logging on file.
        * `loggingpath`: define the path of logging file.
    
2. `server`

3. `client`

###Persistence handlers configuration options

```xml
<handler>
    <class>FilePersistenceHandler</class>
    <filename>Required</filename>
    <filetype>Optional only if the file name has a proper extesion (.csv or .json)</filetype>
    <resourceidcolumn>Required</resourceidcolumn>
    <statuscolumn>Required</statuscolumn>
    <uniqueresourceid>Optional (Default: False)</uniqueresourceid>
    <onduplicateupdate>Optional (Default: False)</onduplicateupdate>
    <savetimedelta>Required</savetimedelta>
</handler>
```

```xml
<handler>
    <class>RolloverFilePersistenceHandler</class>
    <filename>Required</filename>
    <filetype>Optional only if the file name has a proper extesion (.csv or .json)</filetype>
    <resourceidcolumn>Required</resourceidcolumn>
    <statuscolumn>Required</statuscolumn>
    <uniqueresourceid>Optional (Default: False)</uniqueresourceid>
    <onduplicateupdate>Optional (Default: False)</onduplicateupdate>
    <savetimedelta>Required</savetimedelta>
    <sizethreshold>Optional only if amountthreshold is specified (Default: 0)</sizethreshold>
    <amountthreshold>Optional only if sizethreshold is specified (Default: 0)</amountthreshold>
</handler>
```

```xml
<handler>
    <class>MySQLPersistenceHandler</class>
    <user>Required</user>
    <password>Required</password>
    <host>Required</host>
    <name>Required</name>
    <table>Required</table>
    <primarykeycolumn>Required</primarykeycolumn>
    <resourceidcolumn>Required</resourceidcolumn>
    <statuscolumn>Required</statuscolumn>
    <onduplicateupdate>Optional (Default: False)</onduplicateupdate>
</handler>
```

###Filters configuration options
            
```xml
<filter>
    <class>SaveResourcesFilter</class>
    <name>Optional (Default: class name, i.e., SaveResourcesFilter)</name>
    <parallel>Optional (Default: False)</parallel>
    <handler>
        <class>Required</class>
        <!--- Other handler specific configurations--->
    </handler>
</filter>
```

Management
-----

While server is running some management operations can be done through the script `manager.py`. Type 

`python manager.py -h`

to see usage instructions and a summary of all available operations. Bellow is a more detailed description of each operation supported:

1. Remove clients
    
    This operation can be used to disconnect clients in a safe manner or remove from the server's list clients that are already disconnected. In the first case, when a remove request is made the server waits for the client to make contact to signal it for finalization. This way, the client has a chance to complete its current work before execution interruption. It is possible to specify multiple clients at once to be removed, just giving multiple IDs (or hostnames) separated by spaces. When a hostname is given, all clients from that host are removed. It is possible to give a range of IDs of clients that have to be removed too. The range is specified in the form *minID:maxID* (e.g, 2:6 will remove clients with IDs 2 to 6, inclusive). Besides, there are also two keywords that can be used: *disconnected* and *all*. As the names implies, the first keyword removes all disconnected clients and the second keyword removes all clients. IDs, hostnames, ranges and keywords can be used togheter in the command line, separated by spaces.
<br /><br />
2. Reset resources status
	
    Turn all resources in the specified status category into AVAILABLE. For safety, only resources in categories FAILED and ERROR can be reseted while there are clients connected. There must be no client connected to be possible to reset resources in categories SUCCEEDED and INPROGRESS.
<br /><br />
3. Shut down server
	
    Interrupt all data collection process in a safe manner. When a shut down request is made the server waits for each client to make contact to signal it for finalization. This way, all clients have a chance to complete their current work before execution interruption. After all clients have been finished the server shuts down all filters, then the persistence handler and, at last, itself.
<br /><br />
4. Show collection status
    * Basic status (*default*): Show some basic status information about all connected clients as well as a brief information about the whole collection process. If no command line option is choosen when running the manager, this is the option that is used by default.
    * Raw status: Show all status information sent by the server without much concern about information presentation. This option is specially usefull when investigating any possible problems in the collection process.
    * Extended status: Show more complete status information about all connected clients as well as some statistics about the whole collection process (current session and global).

    
Customization
-----

There are three aspects in the data collection program that can be customized: crawler, persistence and filters. You can extend and/or modify these aspects to suit your needs. The crawler must be customized in order to the program to be usefull. Persistence and filters customizations depends on your scenario. 

Taken together, the customizations available makes possible to adapt the program for many different data collection setups.

### Crawler

The program expects that you provide an implementation for the crawl method of a BaseCrawler derived class. The implementation that you provide is the one that actually do the collection of the resource, as this task varies according to resource type, resource origin and other things related to the data that you want to get.

Crawler customizations must be done in the file `crawler.py`. See the internal documention in this module for implementation details.

### Persistence

Persistence customization may be important if the default persistence handlers, available in `persistence.py`, do not fit your scenario. The existence of the persistence layer adds a lot of flexibility in the program, as all issues related to resources storage can be abstracted by the server. If you need, for example, to use a not supported DBMS, you just have to create a persistence handler for it, derived from BasePersistenceHandler or a subclass of it.

The default persistence handlers can also be extended to support specific features. For example, FilePersistenceHandler can easily be extended to support other file types beyond JSON and CSV. 

### Filters

Filters are stored in the file `filters.py` and are a completelly optional part of the program. You can use it, for example, to implement code that have to do some processing for all clients, sharing data among clients or maintain common state. Each filter has methods to execute code before and after the crawling of a resource, so they can also be used to modify, validate or store information at these two moments.

There is one filter that comes by default in the program: SaveResourcesFilter. This filter can be used to store resources sent by the client. Different from server's feedback option, however, this filter does not save the resources in the same place where the original resources are stored. Instead, SaveResourcesFilter uses a persistence handler to take care of low level persistence details. So, it serves also as an example of how you can use persistence handlers in other parts of the code.

    