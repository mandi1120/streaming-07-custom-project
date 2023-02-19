'''
    Amanda Hanway - Streaming Data, Module 7
    Date: 2/4/23

    This program gets posts from reddit.com using the API 
    and then writes the data to a csv file. This is the base code 
    taken from the link below, and was helpful to view how the data 
    is pulled before writing the producer program.

    Reddit API Base Code Source: "How to Use the Reddit API in Python"
    -Link: https://towardsdatascience.com/how-to-use-the-reddit-api-in-python-5e05ddfd1e5c 
'''

######## imports ########
import requests
import pandas as pd
from datetime import datetime


######## declare constants ########

# set reddit connection credentials from a file
# to keep my credentials private 
cred = pd.read_csv('reddit_login_credentials.txt')
username = cred.loc[0][0]
password = cred.loc[1][0]
dev_app_name = cred.loc[2][0]
personal_use_script = cred.loc[3][0]
secret_token = cred.loc[4][0]

# set source page url - latest posts on the r/dataanalysis subreddit 
web_page = "https://oauth.reddit.com/r/dataanalysis/new/"

# set how many posts to return (n * 100 per batch)
post_count = 3


######## define functions ########

def df_from_response(res):
    '''
    Convert responses to a dataframe
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
            'created_utc': datetime.fromtimestamp(post['data']['created_utc']).strftime('%Y-%m-%dT%H:%M:%SZ'),
            'id': post['data']['id'],
            'kind': post['kind']
        })
    
    # convert the list of dictionaries to a dataframe
    df = pd.DataFrame.from_dict(df_list)

    return df

def make_request(un: str=username, pw: str=password, app_nm: str=dev_app_name, pg: str=web_page):
    '''
    Request an OAuth token and connect the reddit api 
    then request data from the webpage and add it to a dataframe
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
    params = {'limit': 100}

    # create a list to store the data
    data_list = []

    # loop through n times (returning n*100 posts)
    for i in range(post_count):
        # make request
        res = requests.get(pg,
                        headers=headers,
                        params=params)

        # get dataframe from response
        new_df = df_from_response(res)
        # take the final row (oldest entry)
        row = new_df.iloc[len(new_df)-1]
        # create fullname
        fullname = row['kind'] + '_' + row['id']
        # add/update fullname in params
        params['after'] = fullname
        
        # append new_df to the data_list
        data_list.append(new_df)   

    # concatenate data_list to the data dataframe
    data = pd.concat(data_list, ignore_index=True)     
    
    # preview data 
    print(data)  
    data.to_csv('data.csv', encoding='utf-8', index=True)


# Standard Python idiom to indicate main program entry point
# This allows us to import this module and use its functions
# without executing the code below.
# If this is the program being run, then execute the code below
if __name__ == "__main__":  

    make_request()

  



