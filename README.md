# IntelliInvest News and Market Summary Automation

Welcome to the IntelliInvest News and Market Summary Automation repository! This project automates the process of gathering and summarizing financial news, and generating pre-market and post-market summaries for the IntelliInvest application.

## Project Overview

During my internship at IntelliInvest, I developed a smart system using Python to automate the tracking and summarization of financial news in real-time. This system connects various tools to collect data from sources like DuckDuckGo and Yahoo Finance. It focuses on key market indices such as Nifty50 and Sensex, providing insights into market sentiment before and after trading hours. Summaries are generated objectively, without personal opinions, tailored specifically for traders and investors.

## Flow for Running the Code

1. Ensure `pure_links.py`, `all_final.py`, and `clean_send.py` are in the same directory.
2. Run 'pure_links.py' at the specified time intervals (specified in the code), 'pure_links.py' will then take care of running 'all_final.py' and 'clean_send.py'.

---

