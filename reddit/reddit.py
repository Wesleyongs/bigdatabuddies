import pandas as pd
from pmaw import PushshiftAPI
import datetime
import time
import re

api = PushshiftAPI(num_workers=20)

def scrape_comments(subreddit, before, after):
    '''
    Returns Pandas dataframe of comments from subreddit within the specified date range in unix timestamp.
        Text data of comments are under 'body' column.
    
    Usage: data = scrape_comments("chatgpt", 1679276829, 1677635229)
        ## before = int(datetime.datetime(2023,3,20,0,0).timestamp())
        ## after = int(datetime.datetime(2023,3,01,0,0).timestamp())
    '''
    main_data = None
    count_of_calls = 0
    start_time = time.time()
    while before > after:
        comments = api.search_comments(subreddit=subreddit, limit=1000, before=before, after=after)
        comments_df = pd.DataFrame(comments)
        try:
            comments_data = comments_df[['subreddit_id', 'subreddit', 'id', 'author', 'body', 'created_utc']]
            if type(main_data) == pd.core.frame.DataFrame:
                main_data = main_data.append(comments_data, ignore_index = True)
            else:
                main_data = comments_data
        except:
            break
        before = comments_df.created_utc.iloc[-1]
        count_of_calls += 1
    
    end_time = time.time()
    elapsed = end_time - start_time
    
    print(f'Retrieved {main_data.shape[0]} comments from Pushshift')
    print(f'Number of calls: {count_of_calls}')
    print(f'Execution time: {time.strftime("%Hh%Mm%Ss", time.gmtime(elapsed))}')
    
    return main_data


def cleanTxt(text):
    '''
    Simple preprocessing for reddit data
    '''
    blacklist = ['**Attention!', 'To avoid redundancy of similar questions in the comments section' \
                , 'Hey , your post has been removed']
    for phrase in blacklist:
        if phrase in text:
            return ''
    text=re.sub(r'@[A-Za-z0-9]+','',text) ## removing @ mention
    text=re.sub(r'#','',text)             ## removing # symbol
    text=re.sub(r'\n','',text)             ## removing \n symbol
#     text=re.sub(r'u/[A-Za-z0-9]+','',text) ## removing /u mention
    text=re.sub(r'RT[\s]+','',text)  ## removing RT followed byspace
    text=re.sub(r'https?:\/\/\S+','',text) ## removing https
    
    return text

def remove_emojis(data):
    '''
    Removing emoji and Unicode
    '''
    emoj = re.compile("["
        u"\U0001F600-\U0001F64F"  # emoticons
        u"\U0001F300-\U0001F5FF"  # symbols & pictographs
        u"\U0001F680-\U0001F6FF"  # transport & map symbols
        u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
        u"\U00002500-\U00002BEF"  # chinese char
        u"\U00002702-\U000027B0"
        u"\U00002702-\U000027B0"
        u"\U000024C2-\U0001F251"
        u"\U0001f926-\U0001f937"
        u"\U00010000-\U0010ffff"
        u"\u2640-\u2642" 
        u"\u2600-\u2B55"
        u"\u200d"
        u"\u23cf"
        u"\u23e9"
        u"\u231a"
        u"\ufe0f"  # dingbats
        u"\u3030"
                      "]+", re.UNICODE)
    return re.sub(emoj, '', data)

if __name__ == "__main__":
    before = int(datetime.datetime(2023,3,19,0,0).timestamp())
    after = int(datetime.datetime(2023,3,18,0,0).timestamp())
    subreddit="chatgpt"
    data = scrape_comments(subreddit, before, after)
    data['body'] = data['body'].apply(cleanTxt)
    data['body'] = data['body'].apply(remove_emojis)
    data.to_csv("chatgpt-comments.csv")


