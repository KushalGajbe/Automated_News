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
import langchain
from langchain_community.llms import Ollama
from langchain_community.tools import DuckDuckGoSearchRun


print('-'*50)
print('Completed imports')
print('-'*50)

# RSS feed URL
rss_url = "https://news.google.com/rss/topics/CAAqKggKIiRDQkFTRlFvSUwyMHZNRGx6TVdZU0JXVnVMVWRDR2dKSlRpZ0FQAQ?hl=en-IN&gl=IN&ceid=IN%3Aenv"

# Parse the RSS feed
feed = feedparser.parse(rss_url)

print('-'*50)
print('Completed parsing RSS feed')
print('-'*50)

# Get the current date and time in IST
now_ist = datetime.now(pytz.timezone('Asia/Kolkata'))


def define_summary_intervals(now_ist):
    return {
        'pre_market_1': (now_ist.replace(hour=4, minute=0, second=0, microsecond=0), now_ist.replace(hour=6, minute=30, second=0, microsecond=0)),
        'pre_market_2': (now_ist.replace(hour=6, minute=30, second=0, microsecond=0), now_ist.replace(hour=8, minute=30, second=0, microsecond=0)),
        'market_hours_1': (now_ist.replace(hour=9, minute=15, second=0, microsecond=0), now_ist.replace(hour=11, minute=30, second=0, microsecond=0)),
        'market_hours_2': (now_ist.replace(hour=11, minute=30, second=0, microsecond=0), now_ist.replace(hour=13, minute=30, second=0, microsecond=0)),
        'market_hours_3': (now_ist.replace(hour=13, minute=30, second=0, microsecond=0), now_ist.replace(hour=15, minute=30, second=0, microsecond=0)),
        'post_market_1': (now_ist.replace(hour=15, minute=30, second=0, microsecond=0), now_ist.replace(hour=15, minute=50, second=0, microsecond=0))
    }

def define_run_intervals(now_ist):
    return {
        'detect_pre_market_1': (now_ist.replace(hour=6, minute=30, second=0, microsecond=0), now_ist.replace(hour=7, minute=0, second=0, microsecond=0)),
        'detect_pre_market_2': (now_ist.replace(hour=8, minute=30, second=0, microsecond=0), now_ist.replace(hour=8, minute=40, second=0, microsecond=0)),
        'detect_market_hours_1': (now_ist.replace(hour=11, minute=30, second=0, microsecond=0), now_ist.replace(hour=12, minute=0, second=0, microsecond=0)),
        'detect_market_hours_2': (now_ist.replace(hour=13, minute=30, second=0, microsecond=0), now_ist.replace(hour=14, minute=0, second=0, microsecond=0)),
        'detect_market_hours_3': (now_ist.replace(hour=15, minute=30, second=0, microsecond=0), now_ist.replace(hour=15, minute=40, second=0, microsecond=0)),
        'detect_post_market_1': (now_ist.replace(hour=15, minute=50, second=0, microsecond=0), now_ist.replace(hour=16, minute=0, second=0, microsecond=0))
    }

def get_summary_interval(now_ist, run_intervals, summary_intervals):
    for run_window, (run_start, run_end) in run_intervals.items():
        if run_start <= now_ist <= run_end:
            if run_window == 'detect_pre_market_1':
                file_to_save="pre_sum1"
                return summary_intervals['pre_market_1'], file_to_save
            elif run_window == 'detect_pre_market_2':
                file_to_save="pre_sum2"
                return summary_intervals['pre_market_2'], file_to_save
            elif run_window == 'detect_market_hours_1':
                file_to_save="market_sum1"
                return summary_intervals['market_hours_1'], file_to_save
            elif run_window == 'detect_market_hours_2':
                file_to_save="market_sum2"
                return summary_intervals['market_hours_2'], file_to_save
            elif run_window == 'detect_market_hours_3':
                file_to_save="market_sum3"
                return summary_intervals['market_hours_3'], file_to_save
            elif run_window == 'detect_post_market_1':
                file_to_save="market_sum4"
                return summary_intervals['post_market_1'], file_to_save
    return None, None

def extract_links(feed, now_ist, summary_start, summary_end):
    links = []

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
links = extract_links(feed, now_ist, summary_start, summary_end)

print('-'*50)
print('Completed extracting links')
print(f"Number of links: {len(links)}")
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

prompt1 = """
I want you to summarize the given input in a precise manner. If possible, please try to get Nifty50, Bank Nifty, Sensex indices levels. The content should get summarized in 250 words maximum.
Please keep only stock market related crucial information from the input provided. Don't give me anything like " Here is the summary" in the answer, just directly
give me the summary. The input is given below:
"""

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


# Extract and summarize the content from the selected links
final_summary = ""
# Define the batch size
batch_size = 3  # Adjust batch size according to your needs

# Iterate through links_to_summarize in batches
for i in range(0, len(links), batch_size):
    batch_links = links[i:i + batch_size]

    print(f'Summarizing batch {i // batch_size + 1}...')

    # Extract article content for the current batch of links and clean the text
    articles = [clean_text(extract_article_content(link)['content']) for link in batch_links]

    print(f'Querying the model for batch {i // batch_size + 1}...')

    # Use articles as prompts to query the model in batch
    summaries = query_model1(prompt1, articles)

    print(f'Completed summarizing batch {i // batch_size + 1}.')

    # Append the summaries to the final_summary
    final_summary += summaries + "\n"

    print('-'*50)

print('-'*50)
print('Completed summarizing all batches')
print('-'*50)

print('Saving the summary...')
# Save the final output in a text file and below it store the final summary
with open(f'{file_to_open}.txt', 'w') as f:
    f.write(final_summary)

print('Summary saved successfully.')