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

<!-- Steps needed to run the crawler, described in a simple way. -->

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
10. Server applies post-processing filters (if there are any filters to apply)
11. Server talks with persistence layer to save the status of the crawling process for that resource
12. Back to step 4


Configuration options
-----

General options

```xml
<?xml version="1.0" encoding="ISO8859-1" ?>
<config>
    <global>
        <connection>
            <address>Required</address>
            <port>Required</port>
        </connection>
        <feedback>Optional (Default: False)</feedback>
        <echo>
            <!--- The echo section itself is optional --->
            <logging>Optional (Default: None)</logging>
            <loggingpath>Optional (Default: None)</loggingpath>
            <verbose>Optional (Default: None)</verbose>
        </echo>
    </global>
    <server>
        <echo>
            <!--- The echo section itself is optional --->
            <logging>Optional (Default: True)</logging>
            <loggingpath>Optional (Default: ".")</loggingpath>
            <verbose>Optional (Default: False)</verbose>
        </echo>
        <loopforever>Optional (Default: False)</loopforever>
        <persistence>
            <handler>
                <class>Required</class>
                <echo>
                    <!--- The echo section itself is optional --->
                    <logging>Optional (Default: True)</logging>
                    <loggingpath>Optional (Default: ".")</loggingpath>
                    <verbose>Optional (Default: False)</verbose>
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
                    <logging>Optional (Default: True)</logging>
                    <loggingpath>Optional (Default: ".")</loggingpath>
                    <verbose>Optional (Default: False)</verbose>
                </echo>
                <!--- Other filter specific configurations --->
            </filter>
        </filtering>
    </server>
    <client>
        <echo>
            <!--- The echo section itself is optional --->
            <logging>Optional (Default: True)</logging>
            <loggingpath>Optional (Default: ".")</loggingpath>
            <verbose>Optional (Default: False)</verbose>
        </echo>
        <crawler>
            <class>Required</class>
            <echo>
                <!--- The echo section itself is optional --->
                <logging>Optional (Default: True)</logging>
                <loggingpath>Optional (Default: ".")</loggingpath>
                <verbose>Optional (Default: False)</verbose>
            </echo>
        </crawler>
    </client>
</config>
```

Persistence handlers options

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

Filters options
            
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

<!-- Describe the manager (and other possible auxiliary scripts) -->

Customization
-----
### Crawler
### Filters
### Persistence

    