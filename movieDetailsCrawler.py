# -*- coding: utf-8 -*-
import scrapy
from scrapy.crawler import CrawlerProcess
import MySQLdb
import threading
import concurrent.futures
from xextract import String, Group
import requests
import json
import re

class ToScrapeCSSSpider(scrapy.Spider):
    name = 'rtCrawler'
    start_urls = []

    page_counter = 0

    measure = {
    'commit_count' : 0,
    'fails' : 0
    }

    movie_details = {
    'identity' : [],
    'description' : [],
    'director' : [],
    'writers' : [],
    'actors' : [],
    'movie_info_url' : [],
    'next_page_url' : ""
    }

    def checking(self, x):
        x = str(x)
        if len(x) == 0 or x.isspace():
            x = None
        else:
            x  = x.replace('"', r'\"')
        return x

    def read_from_table(self):
        MySQLdb.escape_string("'")
        MySQLdb.escape_string('"')
        db = MySQLdb.connect("localhost","root","password","blockbuster", charset = 'utf8', use_unicode = True)
        cursor = db.cursor()
        sql = "SELECT id, movie_info_url FROM blockbuster_predicter WHERE id > '%d' and id <= '%d' and  movie_info_url != 'None'" % (9000,10000)
        # Redo where None is stored for title, keywords, texts, summary, img_url
        #sql = "SELECT id, movie_info_url FROM blockbuster_predicter WHERE id > '%d' and id <= '%d' and texts='None' and title='None' and keywords='None' and summary='None' and img_url ='None'" % (120000,130000)
        print (sql)
        try:
           cursor.execute(sql)
           results = cursor.fetchall()
           for row in results:
              spider.movie_details['identity'].append(row[0])
              spider.movie_details['movie_info_url'].append(row[1])
           print (spider.movie_details['identity'])
           print (spider.movie_details['movie_info_url'])
        except:
           print ("Error: unable to fecth data")
        db.close()

    def __update_tabel__(self):
       MySQLdb.escape_string("'")
       MySQLdb.escape_string('"')
       db = MySQLdb.connect("localhost","root","Iamcrazy123","blockbuster", charset = 'utf8', use_unicode = True)
       cursor = db.cursor()

       for a, b, c, d, e in zip(spider.movie_details['identity'], spider.movie_details['description'], spider.movie_details['director'], spider.movie_details['writers'], spider.movie_details['actors']):

            try:

                b = self.checking(b)
                c = self.checking(c)
                d = self.checking(d)
                e = self.checking(e)

            except Exception as ex:
                template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                message = template.format(type(ex).__name__, ex.args)
                print (message)
                print (a)

            sql = """UPDATE blockbuster_predicter SET description="%s", director="%s", writers="%s", actors="%s" WHERE id=%d""" % (b, c, d, e, a)

            try:
                cursor.execute(sql)
                db.commit()
                print ("Commit")
                print (a)
                spider.measure['commit_count'] = spider.measure['commit_count'] + 1
                print (sql)

            except Exception as ex:
                template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                message = template.format(type(ex).__name__, ex.args)
                #print (message)
                print (sql)
                spider.measure['fails'] = spider.measure['fails'] + 1
                db.rollback()

       db.close()
       print ("Commit Count ", spider.measure['commit_count'])
       print ("Fails ", spider.measure['fails'])


    def display(self):
        print (spider.movie_details['identity'])
        print (spider.movie_details['movie_info_url'])
        print (spider.movie_details['description'])
        print (spider.movie_details['director'])
        print (spider.movie_details['writers'])
        print (spider.movie_details['actors'])

    def check_and_join(self,x):
        if x is not None:
            if len(x) > 1 :
                x = ",".join(x)
            else:
                x = x[0]
        return x

    def parse(self, response):

        if response.css('div.plot_summary '):
            names1 = []
            # names2 = []
            # names3 = []
            print ("Exist")
            print (response)

            spider.movie_details['description'].append(response.css('div.summary_text ::text').extract_first())

            subheading1 = response.css('div.plot_summary > div:nth-child(2) > h4 ::text').extract_first()
            print (subheading1)
            subheading2 = response.css('div.plot_summary > div:nth-child(3) > h4 ::text').extract_first()
            print (subheading2)
            subheading3 = response.css('div.plot_summary > div:nth-child(4) > h4 ::text').extract_first()
            print (subheading3)

            i = 1
            length = len(response.css('div.credit_summary_item'))
            print ("hiiii")
            print (length)
            if length < 4:
                for row in response.css('div.credit_summary_item'):
                    names = row.css('span > a ::text').extract()
                    print (names)
                    name = self.check_and_join(names)
                    if length == 3:
                        if i == 1:
                            spider.movie_details['director'].append(name)
                        elif i == 2:
                            spider.movie_details['writers'].append(name)
                        else:
                            spider.movie_details['actors'].append(name)
                    elif length == 2:
                        if i == 1:
                            spider.movie_details['director'].append(name)
                        else:
                            spider.movie_details['actors'].append(name)
                            spider.movie_details['writers'].append(None)
                    elif length == 1:
                        spider.movie_details['director'].append(name)
                        spider.movie_details['actors'].append(None)
                        spider.movie_details['writers'].append(None)
                    else:
                        spider.movie_details['director'].append(None)
                        spider.movie_details['actors'].append(None)
                        spider.movie_details['writers'].append(None)

                    i += 1
            else:
                print ("Anomoly")
                spider.movie_details['director'].append(None)
                spider.movie_details['actors'].append(None)
                spider.movie_details['writers'].append(None)


            spider.page_counter += 1
            print (spider.page_counter)
            if spider.page_counter < len(spider.movie_details['movie_info_url']):
                next_page_url = "http://www.imdb.com" + spider.movie_details['movie_info_url'][spider.page_counter]
                print ("PAGE: ",spider.page_counter + 1)
                print (next_page_url)
                yield response.follow(next_page_url, self.parse)

        else:
            print ("All Records Read")

    def __return_seed_url__(self):
        return ("http://www.imdb.com" + spider.movie_details['movie_info_url'][0])

if __name__ == "__main__":
    spider = ToScrapeCSSSpider()

    spider.read_from_table()

    seed_url = spider.__return_seed_url__()
    spider.start_urls.append(seed_url)
    print (spider.start_urls)

    process = CrawlerProcess({'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'})
    process.crawl(ToScrapeCSSSpider)
    process.start()

    spider.display()
    spider.__update_tabel__()
