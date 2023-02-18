# Amanda Hanway - Streaming Data, Module 7
- Custom Project: Streaming data using the reddit.com API and RabbitMQ server
- Date: 2/18/23

## Program Overview:
- Producer.py
    - This program gets posts from reddit.com using the reddit API, then streams the post as a message to two queues on the RabbitMQ server.  
    - The following subreddit pages are set as the source:
        - r/dataanalysis: https://oauth.reddit.com/r/dataanalysis/new/
        - r/todayilearned: https://oauth.reddit.com/r/todayilearned/top/?t=day   
- Consumer.py
    - This program listens for messages from two queues on the RabbitMQ server, continuously.
    - It performs transformations on messages when received, and writes the cleaned message to an output file.
    - An alert is generated when a set amount of time has passed between posts.    

## Prerequisites:
- Requres a reddit.com account and API application
    - Account: username, password
    - Application: personal use script, secret tokens, app name
        -  Setup instructions
            - https://www.reddit.com/prefs/apps 
            - create another app...
            - select script
            - fill in a name, description, and redirect uri
    - Credit to James Briggs for API instructions, https://towardsdatascience.com/how-to-use-the-reddit-api-in-python-5e05ddfd1e5c
- Libraries:
    - RabbitMQ server must be installed and running 
    - Requests, pandas, and pika must installed in your active environment
    - Installation instructions:
        - https://www.rabbitmq.com/download.html 
        - https://anaconda.org/conda-forge/pika  
        - https://pandas.pydata.org/docs/getting_started/install.html
        - https://requests.readthedocs.io/en/latest/user/install/
        - https://docs.anaconda.com/anaconda/install/index.html
```
# install rabbitmq on windows using chocolatey
choco install rabbitmq

# install pika on windows using conda-forge
conda install -c conda-forge pika

# install pandas on windows using conda-forge
conda install -c conda-forge pandas

# install requests on windows using conda-forge
conda install -c conda-forge requests
```

## Instructions:
- Producer.py
    - Set your reddit and API credentials
    - Set the desired number of posts to get
    - Set your host name if it is different from localhost
    - Turn on (show_offer=true) or turn off (show_offer=false) asking the user if they'd like to open the RabbitMQ Admin site 
    - Run the program in terminal 1
- Consumer.py
    - Set your host name if it is different from localhost   
    - Run the program in terminal 2
    - Open additional terminals to run the consumer as needed

## Screenshots:

### Producer Program Running in Terminal:
![Program Running](Producer_running.png)

### Consumer Program Running in Terminal:
![Program Running](Consumer_running.png)

### Producer & Consumer Programs Running Concurrently:
![Program Running](Consumer+producer_running.png)

### RabbitMQ Management Console:
![RabbitMQ Admin](RabbitMQ_admin.png)





