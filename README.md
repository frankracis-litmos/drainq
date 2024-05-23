# Drain the Azure queue

This program will read and complete all the messages from an 
Azure Service Bus queue or its associated dead letter queue.

Configuration is done through an `appsettings.local.json` file.
It is designed so that you can create this file once and reuse it
to drain various queues.

The top level of the file is a name you give your connection. 
This will map to an object with the following keys:

* `connectionString`  Connection string for the queue
* `queueName`  Name of the queue
* `deadLetter`   false to clear the main queue, true to clear dead letter
    
To clear the queue, simply pass the name of the connection as the
first parameter.

For example: `DrainQ myQueue`
