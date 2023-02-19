"""
    Amanda Hanway - Streaming Data, Module 7
    Date: 2/4/23

    This program listens for messages from two queues 
    on the RabbitMQ server continuously. 
    It performs transformations on messages when received, 
    writes the cleaned message to an output file, 
    and generates an alert message when specific events occur.

    Author: Amanda Hanway 
    Date: 2/4/23
"""

######## imports ########
import pika
import sys
import time
import csv
import re
from collections import deque
from datetime import datetime 


######## declare constants ########
# set host and queue name
host = "localhost"
queue_name_1 = "dataanalysis_queue"
queue_name_2 = "todayilearned_queue"

# Create empty deques to store that last 2 messages
queue_1_deque = deque(maxlen=2)
queue_2_deque = deque(maxlen=2)


######## define functions ########
def callback_1(ch, method, properties, body):
    """ 
    Define behavior on getting a message from r/dataanalysis/new
    """
    # decode the binary message body to a string
    print(f"\n[x] Received:  {body.decode()}")

    # clean up the text (^=ignore) 
    clean_string1 = re.sub(re.compile('[^a-zA-Z0-9\\\/\.\-\!?& _"'',:;()<>[]+#$%\\*]|_'), '', body.decode())
    # remove new lines 
    clean_string2 = clean_string1.replace("\n", " ")
    # split the message into columns
    timestamp = re.findall(r'tms-start/(.+?)/tms-end', clean_string2)
    subreddit = re.findall(r'sub-start/(.+?)/sub-end', clean_string2)
    flr = re.findall(r'flair-start/(.+?)/flair-end', clean_string2)  
    flair = [flr[0].capitalize() if flr[0] != "None" and flr[0] != "" else "No flair"]
    title_str = re.findall(r'title-start/(.+?)/title-end', clean_string2)
    text_str = re.findall(r'text-start/(.+?)/text-end', clean_string2)

    # add the message to a deque and find time since previous post
    # note: posts are in descending order
    queue_1_deque.append(timestamp[0])
    time_current = datetime.strptime(queue_1_deque[0], '%Y-%m-%d, %H:%M:%S')
    time_compare = datetime.strptime(queue_1_deque[-1], '%Y-%m-%d, %H:%M:%S')    
    time_difference = 0
    if int(format(time_current, '%Y')) > 2000 :
        time_difference = time_current - time_compare  
    hours = int(time_difference.total_seconds() // 3600)
    remaining_secs = time_difference.total_seconds() - (hours * 3600)
    mins = remaining_secs // 60

    fullstring = subreddit + flair + timestamp + [str(int(hours)) + " hr. " + str(int(mins)) + " min. since earlier post"] + title_str + text_str 
    listToStr = ', '.join([str(w) for w in fullstring])   
 
    # write message to output file 
    with open("output_dataanalysis.txt", "a", encoding="utf-8", newline='',) as output_file:
        writer_analysis = csv.writer(output_file)  
        writer_analysis.writerow([listToStr]) 
    
    # generate alert
    if hours < 1:
        print(f" >>>>>>>> ALERT: < 1 Hour Between Posts ({str(int(hours))} hr. {str(int(mins))} min.)")   
    elif hours > 4:
        print(f" >>>>>>>> ALERT: > 4 Hours Between Posts ({str(int(hours))} hr. {str(int(mins))} min.)")   

def callback_2(ch, method, properties, body):
    """ 
    Define behavior on getting a message from r/todayilearned/top/?t=day
    """
    # decode the binary message body to a string
    print(f"\n[x] Received:  {body.decode()}")

    # clean up the text(^=ignore) 
    clean_string1 = re.sub(re.compile('[^a-zA-Z0-9\\\/\.\-\!?& _"'',:;()<>[]+#$%\\*]|_'), '', body.decode())
    # remove new lines 
    clean_string2 = clean_string1.replace("\n", " ")
    # split the message into columns
    timestamp = re.findall(r'tms-start/(.+?)/tms-end', clean_string2)
    subreddit = re.findall(r'sub-start/(.+?)/sub-end', clean_string2)
    title_str = re.findall(r'title-start/(.+?)/title-end', clean_string2)

    # add the message to a deque and find how long since it was posted from now
    # note: posts are not in chronological order
    queue_2_deque.append(timestamp[0])
    time_current = datetime.now()
    time_compare = datetime.strptime(queue_2_deque[-1], '%Y-%m-%d, %H:%M:%S')    
    time_difference = time_current - time_compare
    hours = time_difference.total_seconds() // 3600
    remaining_secs = time_difference.total_seconds() - (hours * 3600)
    mins = remaining_secs // 60

    fullstring = subreddit + timestamp + ["Posted " + str(int(hours)) + " hr. " + str(int(mins)) + " min. ago"] + title_str 
    listToStr = ', '.join([str(w) for w in fullstring])  

    # write message to output file  
    with open("output_todayilearned.txt", "a", newline='', encoding="utf-8") as output_file:
        writer_til = csv.writer(output_file)  
        writer_til.writerow([listToStr]) 

    # generate alert
    if hours < 1:
        print(f" >>>>>>>> ALERT: Posted < 1 Hour Ago ({str(int(hours))} hr. {str(int(mins))} min.)")    
    elif hours > 5:
        print(f" >>>>>>>> ALERT: Posted > 5 Hours Ago ({str(int(hours))} hr. {str(int(mins))} min.)")       

# main function to run the program
def main(hn: str = host, qn1: str = queue_name_1, qn2: str = queue_name_2):
    """ 
    Continuously listen for task messages on a named queue.
    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name_1 (str): the name of the first reddit queue
        queue_name_2 (str): the name of the second reddit queue
    """   
    # when a statement can go wrong, use a try-except block
    try:
        # try this code, if it works, keep going
        # create a blocking connection to the RabbitMQ server
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))

    # except, if there's an error, do this
    except Exception as e:
        print()
        print("ERROR: connection to RabbitMQ server failed.")
        print(f"Verify the server is running on host={hn}.")
        print(f"The error says: {e}")
        print()
        sys.exit(1)

    try:
        # use the connection to create a communication channel
        channel = connection.channel()
        
        # do this once for each queue
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        channel.queue_declare(queue=qn1, durable=True)
        channel.queue_declare(queue=qn2, durable=True)

        # The QoS level controls the # of messages
        # that can be in-flight (unacknowledged by the consumer)
        # at any given time.
        # Set the prefetch count to one to limit the number of messages
        # being consumed and processed concurrently.
        # This helps prevent a worker from becoming overwhelmed
        # and improve the overall system performance. 
        # prefetch_count = Per consumer limit of unaknowledged messages      
        channel.basic_qos(prefetch_count=1) 

        # do this once for each queue
        # configure the channel to listen on a specific queue,  
        # use the callback function for the associated queue/channel,
        # and auto-acknowledge the message  
        channel.basic_consume(queue=qn1, on_message_callback=callback_1, auto_ack=True)
        channel.basic_consume(queue=qn2, on_message_callback=callback_2, auto_ack=True)              

        # print a message to the console for the user
        print(" [*] Ready for work. To exit press CTRL+C")
        print("")

        # start consuming messages via the communication channel
        channel.start_consuming()

    # except, in the event of an error OR user stops the process, do this
    except Exception as e:
        print()
        print("ERROR: something went wrong.")
        print(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        print(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        print("\nClosing connection. Goodbye.\n")
        connection.close()


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":

    main()


