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

# Getting the current date and time in IST
now_ist = datetime.now(pytz.timezone('Asia/Kolkata'))


def read_summary(file_path):
    with open(file_path, 'r') as file:
        return file.read()

pre_summary1 = read_summary('pre_sum1.txt')
pre_summary2 = read_summary('pre_sum2.txt')
post_summary1 = read_summary('market_sum1.txt')
post_summary2 = read_summary('market_sum2.txt')
post_summary3 = read_summary('market_sum3.txt')
post_summary4 = read_summary('market_sum4.txt')
market_headlines = read_summary('market_index_closing.txt')


pre_summaries = pre_summary2 + "\n\n" + pre_summary1
post_summaries = market_headlines + "\n\n" + post_summary3 + "\n\n" + post_summary4

if (now_ist.hour<11):
    all_summaries = pre_summaries
    word = "pre-market"
else:
    all_summaries = post_summaries
    word = "post-market"

print("-"*50)
print(f"All Summaries: {word}")
print("-"*50)

prompt_post="""
I want you to summarize the given post-market information in a short, and precise manner, focusing on key financial market insights. Follow these instructions:

1. Summarize the final levels of Nifty50, Bank Nifty, and Sensex, and how much they have changed from the previous close (if possible). If not available then don't include in the final answer.
2. Highlight the performance of key sectors and notable stocks, including any significant gainers or losers.
3. Include a section on major news events of the day and their impact on the market.
4. Ensure the summary is rich in financial markets and stocks-related news, and avoid unnecessary details. The length should be around 200-300 words.
5. Your answer will be going to real time traders, so make sure to not include anything from your side and strictly stick to the input provided.

The input will be a summary of multiple news articles(which was generated by you itself). Please summarise everything from the given input and don't ask:- "Let me know if you'd like me to proceed with summarizing the rest of the input!". Input is given below:-
"""

prompt_pre="""
I want you to summarize the given pre-market information in a short, and precise manner, focusing on key financial market insights. Follow these instructions:

1. Summarize support and resistance levels of Nifty50, Bank Nifty, and Sensex. If not available then don't include in the final answer.
2. Always include news (taken from the input) on global markets,indian markets, commodities, and currencies.
3. Include what could be today's market sentiment, i.e. bullish, bearish, or neutral, and the reasons behind it.
4. Ensure the summary is rich in financial markets( gold market included) and stocks-related news, and avoid unnecessary details.
5. The length should be around 200-300 words.
6. Strictly speaking, do not include stock recommendations (buying or selling).
7. Your answer will be going to real time traders, so make sure to not include anything from your side and strictly stick to the input provided.

The input will be a summary of multiple news articles(which was generated by you itself). Please summarise everything from the given input and don't ask:- "Let me know if you'd like me to proceed with summarizing the rest of the input!". Input is given below:-

"""

# Function to query the model
def query_model(prompt):
    start= time.time()
    print('Querying the model...')
    response = ollama(prompt)
    end = time.time()
    print('Completed querying the model')
    print(f'Time taken: {end - start:.2f} seconds')
    return response

# Initialize the Ollama instance
ollama = Ollama(base_url='http://localhost:11434', model='llama3')


if now_ist.hour<11:
    prompt = prompt_pre
else:
    prompt = prompt_post


# Get the final output by querying the model
final_output = query_model(prompt + "\n\n" + all_summaries)

# Save the final output to a text file
with open('final_output.txt', 'w') as file:
    file.write(final_output)

