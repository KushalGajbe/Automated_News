
import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from datetime import datetime, timedelta
import pytz
import nltk
from nltk.tokenize import sent_tokenize
import feedparser
import re
import time
from nltk.corpus import stopwords
import yfinance as yf
import pandas as pd
import langchain
from langchain_community.llms import Ollama
from langchain_community.utilities import DuckDuckGoSearchAPIWrapper
from langchain_community.tools import DuckDuckGoSearchResults

print('-'*50)
print('Completed imports')
print('-'*50)

# List of market holidays in YYYY-MM-DD format for year 2024.
market_holidays = [
    "2024-01-22", "2024-01-26", "2024-03-08", "2024-03-25", "2024-03-29",
    "2024-04-11", "2024-04-17", "2024-05-01", "2024-05-20", "2024-06-17",
    "2024-07-17", "2024-08-15", "2024-10-02", "2024-11-01", "2024-11-15",
    "2024-12-25"
]

# Get today's date in YYYY-MM-DD format
ist = pytz.timezone('Asia/Kolkata')
today = datetime.now(ist).strftime('%Y-%m-%d')

# Check if today is a market holiday
if today in market_holidays:
    print("Today is a market holiday. Exiting the code.")
    exit()
else:
    print("Today is not a market holiday. Continuing with the code.")


# RSS feed URL
rss_url = ["https://news.google.com/rss/topics/CAAqKggKIiRDQkFTRlFvSUwyMHZNRGx6TVdZU0JXVnVMVWRDR2dKSlRpZ0FQAQ/sections/CAQiYENCQVNRZ29JTDIwdk1EbHpNV1lTQldWdUxVZENHZ0pKVGlJUENBUWFDd29KTDIwdk1EbDVOSEJ0S2hvS0dBb1VUVUZTUzBWVVUxOVRSVU5VU1U5T1gwNUJUVVVnQVNnQSouCAAqKggKIiRDQkFTRlFvSUwyMHZNRGx6TVdZU0JXVnVMVWRDR2dKSlRpZ0FQAVAB?hl=en-IN&gl=IN&ceid=IN%3Aen",
 "https://news.google.com/rss/topics/CAAqKggKIiRDQkFTRlFvSUwyMHZNRGx6TVdZU0JXVnVMVWRDR2dKSlRpZ0FQAQ?hl=en-IN&gl=IN&ceid=IN%3Aen"]



# Get the current date and time in IST
now_ist = datetime.now(pytz.timezone('Asia/Kolkata'))

#now_ist = now_ist.replace(hour=16, minute=35, second=0, microsecond=0)

# yesterday
yesterday_ist = now_ist - timedelta(days=1)


def define_summary_intervals(now_ist):
    return {
        'pre_market_1': (now_ist.replace(hour=5, minute=0, second=0, microsecond=0), now_ist.replace(hour=6, minute=30, second=0, microsecond=0)),
        'pre_market_2': (now_ist.replace(hour=6, minute=30, second=0, microsecond=0), now_ist.replace(hour=8, minute=15, second=0, microsecond=0)),
        'market_hours_1': (now_ist.replace(hour=9, minute=15, second=0, microsecond=0), now_ist.replace(hour=11, minute=30, second=0, microsecond=0)),
        'market_hours_2': (now_ist.replace(hour=11, minute=30, second=0, microsecond=0), now_ist.replace(hour=13, minute=30, second=0, microsecond=0)),
        'market_hours_3': (now_ist.replace(hour=13, minute=30, second=0, microsecond=0), now_ist.replace(hour=15, minute=30, second=0, microsecond=0)),
        'post_market_1': (now_ist.replace(hour=15, minute=30, second=0, microsecond=0), now_ist.replace(hour=16, minute=30, second=0, microsecond=0))
    }

def define_run_intervals(now_ist):
    return {
        'detect_pre_market_1': (now_ist.replace(hour=6, minute=30, second=0, microsecond=0), now_ist.replace(hour=7, minute=0, second=0, microsecond=0)),
        'detect_pre_market_2': (now_ist.replace(hour=8, minute=15, second=0, microsecond=0), now_ist.replace(hour=8, minute=30, second=0, microsecond=0)),
        'detect_market_hours_1': (now_ist.replace(hour=11, minute=30, second=0, microsecond=0), now_ist.replace(hour=12, minute=0, second=0, microsecond=0)),
        'detect_market_hours_2': (now_ist.replace(hour=13, minute=30, second=0, microsecond=0), now_ist.replace(hour=14, minute=0, second=0, microsecond=0)),
        'detect_market_hours_3': (now_ist.replace(hour=15, minute=30, second=0, microsecond=0), now_ist.replace(hour=15, minute=40, second=0, microsecond=0)),
        'detect_post_market_1': (now_ist.replace(hour=16, minute=30, second=0, microsecond=0), now_ist.replace(hour=16, minute=40, second=0, microsecond=0))
    }

def get_summary_interval(now_ist, run_intervals, summary_intervals):
    for run_window, (run_start, run_end) in run_intervals.items():
        if run_start <= now_ist <= run_end:
            if run_window == 'detect_pre_market_1':
                file_to_save="pre_sum1"
                return *summary_intervals['pre_market_1'], file_to_save
            elif run_window == 'detect_pre_market_2':
                file_to_save="pre_sum2"
                return *summary_intervals['pre_market_2'], file_to_save
            elif run_window == 'detect_market_hours_1':
                file_to_save="market_sum1"
                return *summary_intervals['market_hours_1'], file_to_save
            elif run_window == 'detect_market_hours_2':
                file_to_save="market_sum2"
                return *summary_intervals['market_hours_2'], file_to_save
            elif run_window == 'detect_market_hours_3':
                file_to_save="market_sum3"
                return *summary_intervals['market_hours_3'], file_to_save
            elif run_window == 'detect_post_market_1':
                file_to_save="market_sum4"
                return *summary_intervals['post_market_1'], file_to_save
    return None, None, None

def extract_links(rss_url, now_ist, summary_start, summary_end):
    links = []

    for ru in rss_url:
        feed = feedparser.parse(ru)

        print('-'*50)
        print('Completed parsing RSS feed')
        print('-'*50)

        for entry in feed.entries:
            pub_date = entry.published
            pub_date_dt = datetime.strptime(pub_date, "%a, %d %b %Y %H:%M:%S %Z").replace(tzinfo=pytz.timezone('GMT'))

            link = entry.link

            # Convert publish date to IST
            pub_date_ist = pub_date_dt.astimezone(pytz.timezone('Asia/Kolkata'))

            if summary_start <= pub_date_ist <= summary_end:
                links.append(link)

    return links

#Get summary intervals
summary_intervals = define_summary_intervals(now_ist)

#Get run intervals
run_intervals = define_run_intervals(now_ist)

#Find the summary interval which will get summarized
summary_start, summary_end, file_to_open = get_summary_interval(now_ist, run_intervals, summary_intervals)

if summary_start and summary_end:
    print(f"Summarize interval from {summary_start} to {summary_end}")
else:
    print("No summarization needed at this time.")

#Extract links based on the summary interval
links = extract_links(rss_url, now_ist, summary_start, summary_end)

print('-'*50)
print('Completed extracting links')
print(f"Number of links: {len(links)}")
print('-'*50)

# Using duckduckgosearch
# Check if the time is between 8:20 AM and 8:30 AM
if (now_ist.hour == 8 and now_ist.minute >= 20) or (now_ist.hour == 8 and now_ist.minute < 30):

    l=0

    print("Fetching news related to Nifty50 using DuckDuckGo...")

    wrapper = DuckDuckGoSearchAPIWrapper(region="in-en", time="d", max_results=4)

    search = DuckDuckGoSearchResults(api_wrapper=wrapper, backend="news")

    result = search.run("Nifty50")

    # Extract the relevant fields using regular expressions
    pattern = re.compile(r"title:\s*(?P<title>.*?),\s*link:\s*(?P<link>https?://\S*?),\s*date:\s*(?P<date>\S*?),")
    matches = pattern.findall(result)

    # Store the results in a list of dictionaries
    results = [{"title": match[0], "link": match[1], "date": match[2]} for match in matches]

    # Print the results
    for result in results:
        iso_date_dt = datetime.fromisoformat(result['date'])
        iso_date_dt = iso_date_dt.astimezone(pytz.timezone('GMT'))
        iso_date_dt = iso_date_dt.astimezone(pytz.timezone('Asia/Kolkata'))
        if now_ist.replace(hour=6, minute=0, second=0, microsecond=0)<= iso_date_dt <= summary_end:
            l+=1
            links.insert(0,result['link'])
            print(f"Title:- {result['title']}")
            print(f"Link:- {result['link']}")


    print('-'*50)
    print('Completed fetching news related to Nifty50 from DuckDuckGo')
    print(f"Number of links from DuckDuckGo: {l}")
    print('-'*50)



def extract_article_content(url):
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Extract the title
    title = soup.find('title').get_text() if soup.find('title') else 'No title found'

    # Extract all h1 and h2 tags
    headers = [tag.get_text() for tag in soup.find_all(['h1', 'h2'])]

    # Extract all paragraph tags
    paragraphs = [tag.get_text() for tag in soup.find_all('p')]

    # Combine the headers and paragraphs into a single content string
    content = '\n\n'.join(headers + paragraphs)

    return {
        'title': title,
        'content': content
    }

# Define a function to clean the text
def clean_text(content):
    # Remove any unwanted characters and extra whitespace
    content = re.sub(r'[^\w\s.,!?\'\"-]', '', content)
    content = re.sub(r'\s+', ' ', content)
    return content.strip()

def get_market_timing_prefix():
    current_hour_ist = now_ist.hour
    if current_hour_ist >= 9:
        return "This is post-market summary."
    else:
        return "This is pre-market summary."

prompt1 = f"""
I want you to summarize the given input. For everypoint you will include, try including its reasoning (which should be taken from the input). If possible, please try to get Nifty50, Bank Nifty, and Sensex indices support and resistance levels. The content should be summarized in 80 words maximum. Please keep only stock market, financial market (gold market included) related crucial information from the input provided. Anything you will provide will directly go to real time customers, thus strictly don't give your own suggestions(specially about Nifty50, Bank Nifty and Sensex indices support and resistance levels). Don't give any suggestions from your side and strictly stick to the input provided. The input is given below:
"""
prompt2 = f"""
I want you to summarize the given input. For everypoint you will include, try including its reasoning (which should be taken from the input). If possible, please try to get Nifty50, Bank Nifty, and Sensex indices final closing levels (if you don't get them from input, then no need to include anything about them in the summary). The content should be summarized in 80 words maximum. Please keep only stock market, financial market (gold market included) related crucial information from the input provided. Anything you will provide will directly go to real time customers, thus strictly don't give your own suggestions (specially about Nifty50, Bank Nifty and Sensex indices levels). Don't give any suggestions from your side and strictly stick to the input provided. The input is given below:
"""

# Initialize the Ollama instance
ollama = Ollama(base_url='http://localhost:11434', model='llama3')

# Define a function to query the model
def query_model1(prompt1, prompts):

    start = time.time()
    # Combine prompts into a single input string separated by newlines
    combined_prompt = "\n".join(prompts)

    # Call your summarization model (ollama) with the combined prompt
    response = ollama(prompt1+ '\n\n'+ combined_prompt)
    end = time.time()
    print(f'Time taken for the batch: {end - start:.2f} seconds')
    return response

# Get the current market timing prefix
market_timing_prefix = get_market_timing_prefix()

# Ticker dictionary
ticker_dic = {
   "Nifty_50": "^NSEI",
   "Nifty_Bank": "^NSEBANK",
   "Sensex": "^BSESN"
}

# Define a function to fetch and print data for each ticker
def fetch_ticker_data(ticker_symbol):
    # Fetch historical data for the past 5 days
    data = yf.Ticker(ticker_symbol).history(period="5d")

    # Get today's data
    today_data = data.iloc[-1]
    today_close = today_data['Close']
    today_high = today_data['High']
    today_low = today_data['Low']
    today_open = today_data['Open']

    # Get yesterday's data
    yesterday_data = data.iloc[-2]
    yesterday_close = yesterday_data['Close']
    # yesterday_high = yesterday_data['High']
    # yesterday_low = yesterday_data['Low']

    # Calculate % change and difference
    pct_change = (today_close - yesterday_close) / yesterday_close * 100
    diff = today_close - yesterday_close

    return {
        "today_close": today_close,
        "today_high": today_high,
        "today_low": today_low,
        "today_open": today_open,
        "yesterday_close": yesterday_close,
        # "yesterday_high": yesterday_high,
        # "yesterday_low": yesterday_low,
        "pct_change": pct_change,
        "diff": diff
    }

if (now_ist.hour == 16 and now_ist.minute >= 30) or (now_ist.hour == 16 and now_ist.minute <= 40):
    print("Fetching news related to Nifty50 using DuckDuckGo...")
    print(f"Summary interval from {summary_start} to {summary_end}")


    wrapper = DuckDuckGoSearchAPIWrapper(region="in-en", time="d", max_results=4)

    search = DuckDuckGoSearchResults(api_wrapper=wrapper, backend="news")

    result = search.run("Nifty50")

    # Extract the relevant fields using regular expressions
    pattern = re.compile(r"title:\s*(?P<title>.*?),\s*link:\s*(?P<link>https?://\S*?),\s*date:\s*(?P<date>\S*?),")
    matches = pattern.findall(result)

    # Store the results in a list of dictionaries
    results = [{"title": match[0], "link": match[1], "date": match[2]} for match in matches]

    # Print the results
    for result in results:
        iso_date_dt = datetime.fromisoformat(result['date'])
        iso_date_dt = iso_date_dt.astimezone(pytz.timezone('GMT'))
        iso_date_dt = iso_date_dt.astimezone(pytz.timezone('Asia/Kolkata'))

        #Include only the news which are within the 12:00 pm and 4:30 pm IST
        if now_ist.replace(hour=12, minute=0, second=0, microsecond=0)<=iso_date_dt <= summary_end:
            links.append(result['link'])
            print(f"Title:- {result['title']}")
            print(f"Link:- {result['link']}")

    print('-'*50)
    print('Completed fetching news related to Nifty50 from DuckDuckGo for post-market summary')
    print('-'*50)

    print("Getting data from yahoo finance for Nifty50, Bank Nifty, and Sensex...")
    # Store results for each ticker
    ticker_results = {}

    # Loop through the ticker dictionary and fetch data
    for name, ticker in ticker_dic.items():
        data = fetch_ticker_data(ticker)
        ticker_results[name] = data
    print("Completed fetching data from yahoo finance for Nifty50, Bank Nifty, and Sensex")
    print('-'*50)

    todays_headlines = f"""
    Here are today's :- {now_ist} financial market main indices closing values and changes from the previous close:
    1. Nifty50 closes at {ticker_results["Nifty_50"]["today_close"]} points, with a change of {ticker_results["Nifty_50"]["today_close"]:.2f}% ({ticker_results["Nifty_50"]["diff"]:.2f} points) from the previous close.
    2. Bank Nifty closes at {ticker_results["Nifty_Bank"]["today_close"]} points, with a change of {ticker_results["Nifty_Bank"]["today_close"]:.2f}% ({ticker_results["Nifty_Bank"]["diff"]:.2f} points) from the previous close.
    3. Sensex closes at {ticker_results["Sensex"]["today_close"]} points, with a change of {ticker_results["Sensex"]["today_close"]:.2f}% ({ticker_results["Sensex"]["diff"]:.2f} points) from the previous close.

    Please include them in the summary as these are the only correct values for the main indices levels. If you get any news on these specific values ahead, then make sure to include such news in the summary. Further input is provided below:-
    """

    #Save the headlines in a text file
    with open('/home/inteluat/automated_news/market_index_closing.txt', 'w') as f:
        f.write(todays_headlines)




# Extract and summarize the content from the selected links
final_summary = ""
# Define the batch size
def get_batch_size(t):
    if 8<= t <=9:
        return 2
    else:
        return 3

batch_size = get_batch_size(now_ist.hour)  # Adjust batch size according to your needs

print("-"*50)
print(f"Batch size:- {batch_size}")
print("-"*50)

# Iterate through links_to_summarize in batches
for i in range(0, len(links), batch_size):
    batch_links = links[i:i + batch_size]

    print(f'Summarizing batch {i // batch_size + 1}...')

    # Extract article content for the current batch of links and clean the text
    articles = [clean_text(extract_article_content(link)['content']) for link in batch_links]

    print(f'Querying the model for batch {i // batch_size + 1}...')

    if now_ist.hour<10:

        prompt = prompt1
    else:
        prompt = prompt2

    # Use articles as prompts to query the model in batch
    summaries = query_model1(prompt, articles)

    print(f'Completed summarizing batch {i // batch_size + 1}.')

    # Append the summaries to the final_summary
    final_summary += summaries + "\n"

    print('-'*50)

print('-'*50)
print('Completed summarizing all batches')
print('-'*50)

print('Saving the summary...')
# Save the final output in a text file and below it store the final summary
with open(f'/home/inteluat/automated_news/{file_to_open}.txt', 'w') as f:
    f.write(final_summary)

print('Summary saved successfully.')


# If current time is between 8:15 am and 10:00 am, then run all_final.py, then run clean_send.py
if (now_ist.hour == 8 and now_ist.minute >= 15) or (now_ist.hour==9 and now_ist.minute<=59):
    print("Running all_final.py...")
    exec(open("/home/inteluat/automated_news/all_final.py").read())
    print("Completed running all_final.py")
    print('-'*50)
    print("Running clean_send.py...")
    exec(open("/home/inteluat/automated_news/clean_send.py").read())
    print("Completed running clean_send.py")

# Similarly if time is between 16:30 and 17:40, then run all_final.py, then run clean_send.py

if (now_ist.hour == 16 and now_ist.minute >= 30) or (now_ist.hour == 17 and now_ist.minute <= 40):
    print("Running all_final.py...")
    exec(open("/home/inteluat/automated_news/all_final.py").read())
    print("Completed running all_final.py")
    print('-'*50)
    print("Running clean_send.py...")
    exec(open("/home/inteluat/automated_news/clean_send.py").read())
    print("Completed running clean_send.py")

print('-'*50)


# pure_links.py
with open('/home/inteluat/automated_news/pure_cron.txt', 'a') as f:
    f.write(f'Cron job executed at {datetime.now()}\n')
