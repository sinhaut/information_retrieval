# information_retrieval
CS6200

This is a crawler built for vertical searching: 
https://course.ccs.neu.edu/cs6200f20/assignments/3.html

crawler.py - Crawls through 40000 docs starting at seeds for hurricanes
- Multithreaded and checks for politeness policy in robots.txt files

inlinks.py - determines all the inlinks given an outlinks file

index.py - Indexes all crawled webpages, their outlinks and inlinks into Elastic Search

