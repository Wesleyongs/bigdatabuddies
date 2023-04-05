import pandas as pd
from pmaw import PushshiftAPI
import datetime
import time
import re
import boto3

api = PushshiftAPI(num_workers=20)

def scrape_comments(subreddit, after, before):
    '''
    Returns Pandas dataframe of comments from subreddit within the specified date range and saves df into a csv file
        Text data of comments are under 'body' column.
    
    Usage: data = scrape_comments("chatgpt", "2023/03/01", "2023/03/02")
    '''
    format = '%Y/%m/%d'
    before_dt = datetime.datetime.strptime(before, format)
    after_dt = datetime.datetime.strptime(after, format)
    before_utc = int(before_dt.timestamp())
    after_utc = int(after_dt.timestamp())
    
    filename = 'reddit-' + str(after_dt.date()) + '.csv'
    
    main_data = None
    count_of_calls = 0
    start_time = time.time()
#     while before_utc > after_utc :
    while count_of_calls < 30:
        comments = api.search_comments(subreddit=subreddit, limit=1000, before=before_utc, after=after_utc)
        comments_df = pd.DataFrame(comments)
        try:
            comments_data = comments_df[['created_utc', 'subreddit_id', 'subreddit', 'id', 'body', 'author']]
            if type(main_data) == pd.core.frame.DataFrame:
                main_data = main_data.append(comments_data, ignore_index = True)
            else:
                main_data = comments_data
        except:
            break
        before_utc = comments_df.created_utc.iloc[-1]
        count_of_calls += 1
    
    end_time = time.time()
    elapsed = end_time - start_time
    
    print(f'Retrieved {main_data.shape[0]} comments from Pushshift')
    print(f'Number of calls: {count_of_calls}')
    print(f'Execution time: {time.strftime("%Hh%Mm%Ss", time.gmtime(elapsed))}')
    
    # Drop rows with comments from Reddit moderators
    main_data = main_data[(main_data.author != 'AutoModerator') & (main_data.author != '[deleted]')]
    # Convert columns to string
    main_data['subreddit_id'] = main_data['subreddit_id'].astype(str)
    main_data['subreddit'] = main_data['subreddit'].astype(str)
    main_data['id'] = main_data['id'].astype(str)
    main_data['author'] = main_data['author'].astype(str)
    main_data['created_utc'] = main_data['created_utc'].apply(lambda x: datetime.datetime.fromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S'))
    main_data['created_utc'] = main_data['created_utc'].astype(str)
    main_data['body'] = main_data['body'].astype(str)
    # Remove all non-letter characters using regex
    main_data['body'] = main_data['body'].apply(lambda x: re.sub('[^a-zA-Z]', ' ', x))
    
    main_data.to_csv(filename, encoding='utf-8', index=False)
    
    # Upload the CSV file to S3
    aws_access_key_id='AKIARJIOGROB7MVFOYGX'
    aws_secret_access_key='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'
    bucket_name = 'handsome-bucket-reddit'

    s3 = boto3.resource('s3',
                        aws_access_key_id=aws_access_key_id,
                        aws_secret_access_key=aws_secret_access_key,
                        region_name='us-east-1')
    bucket = s3.Bucket(bucket_name)
    
    s3_folder_name = 'read/'
    s3_path = s3_folder_name + filename
    with open(filename, 'rb') as file:
        bucket.put_object(Key=s3_path, Body=file)
        
    print(f'Saved {filename} to S3 bucket {bucket_name} under {s3_path}')
            
    return main_data

if __name__ == '__main__':

    # Calculate dates
    today = datetime.datetime.today()
    yesterday = today - datetime.timedelta(days=1)

    # Format dates as strings
    today_str = today.strftime('%Y/%m/%d')
    yesterday_str = yesterday.strftime('%Y/%m/%d')

    # Scrape comments for subreddit "chatgpt" for the previous day
    scrape_comments("chatgpt", yesterday_str, today_str)