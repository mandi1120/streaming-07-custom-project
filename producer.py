'''
    Amanda Hanway - Streaming Data, Module 7
    2/4/23

    This program gets posts from reddit.com using the reddit api 
    then streams the post as a message to two queues on the RabbitMQ server.

    Author: Amanda Hanway 
    Date: 2/4/23

    Reddit API Base Code Source:  
    - Link https://towardsdatascience.com/how-to-use-the-reddit-api-in-python-5e05ddfd1e5c 
'''

######## imports ########
import requests
import pandas as pd
from datetime import datetime
import pika
import sys
import webbrowser
import time


######## declare constants ########

# set reddit connection credentials from a file
# to keep my credentials private 
cred = pd.read_csv('reddit_login_credentials.txt')
username = cred.loc[0][0]
password = cred.loc[1][0]
dev_app_name = cred.loc[2][0]
personal_use_script = cred.loc[3][0]
secret_token = cred.loc[4][0]

# set source page url 
# latest posts on the r/dataanalysis subreddit 
web_page_1 = "https://oauth.reddit.com/r/dataanalysis/new/"
# top posts on the r/todayilearned subreddit
web_page_2 = "https://oauth.reddit.com/r/todayilearned/top/?t=day"

# set how many posts to return 
post_count = 100

# set host and queue name
host = "localhost"
queue_name_1 = "dataanalysis_queue"
queue_name_2 = "todayilearned_queue"

# set to turn on (true) or turn off (false) asking the 
# user if they'd like to open the RabbitMQ Admin site 
show_offer = False


######## define functions ########

def offer_rabbitmq_admin_site(show_offer: str=show_offer):
    """
    Offer to open the RabbitMQ Admin website
    """
    if show_offer == True:
        ans = input("Would you like to monitor RabbitMQ queues? y or n:  ")
        print()
        if ans.lower() == "y":
            webbrowser.open_new("http://localhost:15672/#/queues")
            print()

def delete_queue(host: str, queue_name: str):    
    """
    Delete a queue to clear un-needed messages
    if the code had been run previously
    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
    """
    # create a blocking connection to the RabbitMQ server
    conn = pika.BlockingConnection(pika.ConnectionParameters(host))
    # use the connection to create a communication channel
    ch = conn.channel()
    # delete the queue
    ch.queue_delete(queue=queue_name)    

def send_message(host: str, queue_name: str, message: str):
    """
    Create and send a message to the queue each execution.
    This process runs and finishes.
    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """
    try:       
        # create a blocking connection to the RabbitMQ server
        conn = pika.BlockingConnection(pika.ConnectionParameters(host))
        # use the connection to create a communication channel
        ch = conn.channel()
        # use the channel to declare a durable queue
        # a durable queue will survive a RabbitMQ server restart
        # and help ensure messages are processed in order
        # messages will not be deleted until the consumer acknowledges
        ch.queue_declare(queue=queue_name, durable=True)
        # use the channel to publish a message to the queue
        # every message passes through an exchange
        ch.basic_publish(exchange="", routing_key=queue_name, body=message)
        # print a message to the console for the user
        print(f" [x] Sent {message}\n")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error: Connection to RabbitMQ server failed: {e}")
        sys.exit(1)
    finally:
        # close the connection to the server
        conn.close()

def df_from_response(res):
    '''
    Convert the response for a post to a dataframe
    '''
    # initialize temp dataframe for batch of data in response
    df = pd.DataFrame()

    # loop through each post pulled from res and append to df_list
    df_list = []
    for post in res.json()['data']['children']:
        df_list.append({     
            'subreddit': post['data']['subreddit'],
            'title': post['data']['title'],
            'selftext': post['data']['selftext'],
            'upvote_ratio': post['data']['upvote_ratio'],
            'ups': post['data']['ups'],
            'downs': post['data']['downs'],
            'score': post['data']['score'],
            'link_flair_css_class': post['data']['link_flair_css_class'],
            'created_utc': datetime.fromtimestamp(post['data']['created_utc']).strftime('%Y-%m-%d, %H:%M:%S'),
            'id': post['data']['id'],
            'kind': post['kind']
        })
    
    # convert the list of dictionaries to a dataframe
    df = pd.DataFrame.from_dict(df_list)

    return df

# main function to run the program
def main(un: str=username, pw: str=password, app_nm: str=dev_app_name):
    '''
    Request an OAuth token and connect the reddit api 
    then request data from the webpage and add it to a dataframe
    then create a message and send to the queue
    Parameters:
        username (str): reddit.com username
        password (str): reddit.com password
        dev_app_name (str): name of reddit.com api application
    '''
    # authenticate API
    client_auth = requests.auth.HTTPBasicAuth(personal_use_script, secret_token)
    data = {
        'grant_type': 'password',
        'username': un,
        'password': pw
    }
    headers = {'User-Agent': app_nm}

    # send authentication request for OAuth token
    res = requests.post('https://www.reddit.com/api/v1/access_token',
                        auth=client_auth, data=data, headers=headers)
    # extract token from response and format correctly
    token = f"bearer {res.json()['access_token']}"
    # update API headers with authorization (bearer token)
    headers = {**headers, **{'Authorization': token}}

    # initialize dataframe and parameters for pulling data in loop
    data = pd.DataFrame()
    params = {'limit': post_count}

    # loop through n times for each webpage
    for i in range(post_count):
        for p in [web_page_1, web_page_2]:
            # make request
            res = requests.get(p,
                            headers=headers,
                            params=params)

            # set the queue name
            queue_name = queue_name_1 if p == web_page_1 else queue_name_2

            # get dataframe from response
            data = df_from_response(res)

            # create a message for the row of data 
            fstring_message = f"tms-start/{data.loc[i]['created_utc']}/tms-end"\
                            f", sub-start/{data.loc[i]['subreddit']}/sub-end"\
                            f", flair-start/{data.loc[i]['link_flair_css_class']}/flair-end"\
                            f", title-start/{data.loc[i]['title']}/title-end"\
                            f", text-start/{data.loc[i]['selftext']}/text-end"

            # prepare a binary (1s and 0s) message to stream
            message = fstring_message.encode()

            # send the message
            send_message(host, queue_name, message)

            # wait 2 seconds
            time.sleep(2)
 

# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  

    # if show_offer is turned on (True) then
    # ask the user if they'd like to open the RabbitMQ Admin site 
    offer_rabbitmq_admin_site() 

    # delete the queue if run previously
    delete_queue(host, queue_name_1)
    delete_queue(host, queue_name_2)

    # get the message from the webpage
    # send the message to the queue
    main()

  



