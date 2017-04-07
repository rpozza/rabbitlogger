#!/usr/bin/env python
__author__ = 'Riccardo Pozza, r.pozza@surrey.ac.uk'

import logging
import logging.handlers
import argparse
import sys
import pika
import json
import swiftclient
import csv
import datetime
import os.path

previouspathname = None
previousobjectname = None

def main():
    # Defaults
    LOG_FILENAME = "/var/log/rabbitmq-objstore.log"
    LOG_LEVEL = logging.INFO  # i.e. could be also "DEBUG" or "WARNING"

    # Define and parse command line arguments
    parser = argparse.ArgumentParser(description="RabbitMQ to Object Storage Logging Service")
    parser.add_argument("-l", "--log", help="file to write log to (default '" + LOG_FILENAME + "')")

    # If the log file is specified on the command line then override the default
    args = parser.parse_args()
    if args.log:
        LOG_FILENAME = args.log

    # Configure logging to log to a file, making a new file at midnight and keeping the last 3 day's data
    # Give the logger a unique name (good practice)
    logger = logging.getLogger(__name__)
    # Set the log level to LOG_LEVEL
    logger.setLevel(LOG_LEVEL)
    # Make a handler that writes to a file, making a new file at midnight and keeping 3 backups
    handler = logging.handlers.TimedRotatingFileHandler(LOG_FILENAME, when="midnight", backupCount=3)
    # Format each log message like this
    formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
    # Attach the formatter to the handler
    handler.setFormatter(formatter)
    # Attach the handler to the logger
    logger.addHandler(handler)

    # Replace stdout with logging to file at INFO level
    sys.stdout = MyLogger(logger, logging.INFO)
    # Replace stderr with logging to file at ERROR level
    sys.stderr = MyLogger(logger, logging.ERROR)

    # Setting up Openstack Object Storage connection parameters
    with open('/usr/local/bin/rabbitlogger/myconfig.json') as config_file:
        config_data = json.load(config_file)

    _authurl = 'http://' + config_data["OpStController"] + ':5000/v2.0/'
    _auth_version = '2'
    _user = config_data["OpStObjUsername"]
    _key = config_data["OpStObjPassword"]
    _tenant_name = config_data["OpStObjTenantname"]
    _container = 'sensordump'

    # Connecting to the Object Storage Service
    print "Connecting to Openstack Object Storage Service"
    _swiftconn = swiftclient.Connection(authurl=_authurl, user=_user, key=_key, tenant_name=_tenant_name,
                                   auth_version=_auth_version)
    resp_headers, containers = _swiftconn.get_account()
    print "Connected to Openstack Object Storage Service!"

    # Check if present or create a new container
    if not containers:
        # create container for dump data
        _swiftconn.put_container(_container)
        print "New Container Created!"
    else:
        # check it exists
        found = 0
        for container in containers:
            if container['name'] == _container:
                found = 1
        # create if doesn't
        if not found:
            _swiftconn.put_container(_container)
            print "New Container Created!"

    # RabbitMQ Credentials and Connection to Broker
    print "Connection to RabbitMQ Message Broker"
    usercred = pika.PlainCredentials(config_data["RabbitUsername"], config_data["RabbitPassword"])
    connection = pika.BlockingConnection(pika.ConnectionParameters(config_data["RabbitHost"],config_data["RabbitPort"],'/',usercred))
    channel = connection.channel()
    print "Connected to RabbitMQ Message Broker"

    # need to declare exchange also in subscriber (fanout exchange)
    channel.exchange_declare(exchange='json_log', type='fanout')

    print "Creating a temporary Message Queue"
    # temporary queue name, closed when disconnected
    result = channel.queue_declare(exclusive=True)
    queue_name = result.method.queue

    # bind to the queue considered attached to logs exchange
    channel.queue_bind(exchange='json_log', queue=queue_name)
    print "Bound to the temporary Message Queue"

    print "Waiting for messages to Consume!"
    channel.basic_consume(lambda ch, method, properties, body:callback_ext_arguments(
                                 ch, method, properties, body,swiftconn=_swiftconn, container=_container), queue=queue_name, no_ack=True)
    channel.start_consuming()

def callback_ext_arguments(ch, method, properties, body, swiftconn, container):
    global previousobjectname
    global previouspathname
    try:
        json_parsed = json.loads(body)
    except:
        # if not json packet, discard packet
        return
    data_row = []
    data_row.append(str(json_parsed['ts']))
    try:
        for i in json_parsed['val']['resources']:
            if i['id'] == 5700 or i['id'] == 5547:
                data_row.append(str(json_parsed['ep'] + json_parsed['pth'] + "/" + str(i['id'])))
                data_row.append(str(i['value']))
    except:
        #if cannot parse, discard packet
        return

    try:
        timestamped = datetime.datetime.strptime(str(json_parsed['ts']),"%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        timestamped = datetime.datetime.strptime(str(json_parsed['ts']), "%Y-%m-%dT%H:%M:%SZ")
    suffix = ((timestamped.hour * 60) + timestamped.minute) // 15
    pathname = str(container) + "/" + str(timestamped.year) + "-" + str(timestamped.month) + "-" + str(timestamped.day)
    objectname = str(timestamped.year) + "-" + str(timestamped.month) + "-" + str(timestamped.day) + "-" + str(suffix) + ".csv"
    # look if file exist locally
    if not os.path.exists("/tmp/" + objectname):
        # new file!! record the previous one if exists locally
        if previousobjectname is not None:
            if os.path.exists("/tmp/" + previousobjectname):
                print "Record previous file in the cloud and delete"
                filename = open("/tmp/" + previousobjectname, "r")
                swiftconn.put_object(previouspathname, previousobjectname, contents=filename, content_type='text/plain')
                filename.close()
                # delete locally
                os.remove("/tmp/" + previousobjectname)
        # look if file exist in the cloud
        try:
            resp_headers = swiftconn.head_object(pathname, objectname)
            # not local, but in the cloud
            print "Recover previous file from the cloud and add new data"
            resp_headers, obj_contents = swiftconn.get_object(pathname, objectname)
            filename = open("/tmp/" + objectname,'w')
            filename.write(obj_contents)
            filename.close()
        except swiftclient.ClientException as e:
            if e.http_status == 404:
                # create local file and upload
                print "Create new files local and in the cloud"
                filename = open("/tmp/" + objectname,"w")
                filename.close()
                filename = open("/tmp/" + objectname, "r")
                swiftconn.put_object(pathname, objectname, contents=filename, content_type='text/plain')
                filename.close()

    # now file exists, add data
    filename = open("/tmp/" + objectname, "a")
    fw = csv.writer(filename)
    fw.writerow(data_row)
    filename.close()
    # save previous objectname and pathname
    previouspathname = pathname
    previousobjectname = objectname

# Make a class we can use to capture stdout and sterr in the log
class MyLogger(object):
        def __init__(self, logger, level):
                """Needs a logger and a logger level."""
                self.logger = logger
                self.level = level

        def write(self, message):
                # Only log if there is a message (not just a new line)
                if message.rstrip() != "":
                        self.logger.log(self.level, message.rstrip())

if __name__ == '__main__':
    main()
