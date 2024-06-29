import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from datetime import datetime, timedelta
import pytz
import nltk
from nltk.tokenize import sent_tokenize
import feedparser
import re
from nltk.corpus import stopwords
import langchain
from langchain.llms import Ollama

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

# Define the time windows in IST
today_5_am_ist = now_ist.replace(hour=5, minute=0, second=0, microsecond=0)
today_8_am_ist = now_ist.replace(hour=8, minute=15, second=0, microsecond=0)
today_9_15_am_ist = now_ist.replace(hour=9, minute=15, second=0, microsecond=0)
today_4_30_pm_ist = now_ist.replace(hour=16, minute=30, second=0, microsecond=0)

pre_links = []
post_links = []

# Extract the links and separate them based on the publish date in IST
for entry in feed.entries:
    pub_date = entry.published
    pub_date_dt = datetime.strptime(pub_date, "%a, %d %b %Y %H:%M:%S %Z").replace(tzinfo=pytz.timezone('GMT'))

    link = entry.link

    # Convert publish date to IST
    pub_date_ist = pub_date_dt.astimezone(pytz.timezone('Asia/Kolkata'))

    if yesterday_5_30_pm_ist <= pub_date_ist < today_8_am_ist:
        pre_links.append(link)
    elif today_9_15_am_ist <= pub_date_ist < today_4_30_pm_ist:
        post_links.append(link)

print('-'*50)
print('Completed separating links')
print('-'*50)

# Define a function to extract the article content
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



# Initialize the Ollama instance
ollama = Ollama(base_url='http://localhost:11434', model='llama3')

print('-'*50)
print('Completed initializing Ollama')
print('-'*50)

def get_market_timing_prefix():
    current_hour_ist = now_ist.hour
    if current_hour_ist >= 12:
        return "This is post-market summary."
    else:
        return "This is pre-market summary."


prompt1 = """
I want you to summarize the given input in a short, crisp, and precise manner. If possible, please try to get Nifty50, Bank Nifty, Sensex indices levels. The content should get summarized in 200 words maximum.
Please keep only stock market related crucial information from the input provided. Don't give me anything like " Here is the summary" in the answer, just directly
give me the summary. The input is given below:
"""

# Define a function to query the model
def query_model1(prompt1, prompts):
    # Combine prompts into a single input string separated by newlines
    combined_prompt = "\n".join(prompts)

    # Call your summarization model (ollama) with the combined prompt
    response = ollama(prompt1+ '\n\n'+ combined_prompt)

    return response

def query_model(prompt):
    print('Querying the model...')
    response = ollama(prompt)
    print('Completed querying the model')
    return response

# prompt for summarization
prompt_pre = """
I want you to summarize the given pre-market information in a short, crisp, and precise manner, focusing on key financial market insights. Follow these instructions:

1. Identify and provide support and resistance levels for major indices such as Nifty50, Bank Nifty, and Sensex.
2. Predict the general trend in the market today based on the input provided, highlighting key factors driving this trend.
3. Include any significant news or events that might impact the market today, such as economic reports, corporate earnings, geopolitical events, or major policy changes.
4. Highlight key sectors to watch and any notable stock movements or pre-market activities.
5. Keep the summary focused on actionable insights and avoid redundant details. The length should be between 200-300 words.
6. If possible, add sentiment meter:- people's sentiments about today's market.
7. Do not give any stock recommendations.
The input will be a summary of news articles. Be flexble in giving summary. Rather than including everything, please include more important information in the final output.
Please summarise everything from the given input and don't ask:- "Let me know if you'd like me to proceed with summarizing the rest of the input!".

"""

prompt_post="""
I want you to summarize the given post-market information in a short, crisp, and precise manner, focusing on key financial market insights. Follow these instructions:

1. Summarize the final levels of Nifty50, Bank Nifty, and Sensex, and how much they have changed from the previous close.
2. Highlight the performance of key sectors and notable stocks, including any significant gainers or losers.
3. Include a section on major news events of the day and their impact on the market, such as economic data releases, corporate earnings, mergers and acquisitions, or geopolitical developments.
4. Provide analysis on market sentiment and factors that influenced investor behavior today.
5. Conclude with a brief outlook for the next trading day, mentioning any upcoming events or trends to watch.
6. Ensure the summary is rich in financial markets and stocks-related news, and avoid unnecessary details. The length should be around 200-300 words.
7. You can be flexible about the above points, but please try including the first one if possible. Overall, give a good output.
The input will be a summary of news articles. Be flexble in giving summary. Rather than including everything, please include more important information in the final output.
Please summarise everything from the given input and don't ask:- "Let me know if you'd like me to proceed with summarizing the rest of the input!".
"""



# Get the current market timing prefix
market_timing_prefix = get_market_timing_prefix()

# Decide which links to summarize based on the time of the day in IST
if now_ist.hour >= 16:  # Assuming post-market time is after 4:30 PM IST
    links_to_summarize = post_links
else:  # Pre-market time
    links_to_summarize = pre_links

# Extract and summarize the content from the selected links
final_summary = ""
# Define the batch size
batch_size = 3  # Adjust batch size according to your needs

# Iterate through links_to_summarize in batches
for i in range(0, len(links_to_summarize), batch_size):
    batch_links = links_to_summarize[i:i + batch_size]

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

print('Final Summary:')
# Getting the final output
final_output = query_model( prompt_pre if now_ist.hour < 13 else prompt_post + "\n\n" + market_timing_prefix + "\n\n" + final_summary)

print('Saving the final output...')
# Save the final output in a text file and below it store the final summary
with open('final_output2.txt', 'w') as f:
    f.write(final_output)
    f.write("\n\n\n")
    f.write(final_summary)
