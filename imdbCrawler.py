# -*- coding: utf-8 -*-
import scrapy
from scrapy.crawler import CrawlerProcess
import MySQLdb
import threading
import concurrent.futures

class ToScrapeCSSSpider(scrapy.Spider):
    name = 'rtCrawler'
    start_urls = []

    page_counter = 1

    measure = {
    'commit_count' : 0,
    'fails' : 0
    }

    movie_details = {
    'title' : [],
    'genre' : [],
    'rating' : [],
    'gross' : [],
    'votes' : [],
    'movie_info_url': [],
    'next_page_url' : ""
    }

    def checking(self, x):
        x = str(x)
        if len(x) == 0 or x.isspace():
            x = None
        else:
            x  = x.replace('"', r'\"')
        return x

    def __insert_into_tabel__(self):
       MySQLdb.escape_string("'")
       MySQLdb.escape_string('"')
       db = MySQLdb.connect("localhost","root","password","blockbuster", charset = 'utf8', use_unicode = True)
       cursor = db.cursor()

       for a, b, c, d, e, f in zip(spider.movie_details['title'], spider.movie_details['genre'], spider.movie_details['rating'], spider.movie_details['gross'], spider.movie_details['votes'], spider.movie_details['movie_info_url']):

            try:
                a = self.checking(a)
                b = self.checking(b)
                c = self.checking(c)
                d = self.checking(d)
                if d is not None:
                    d = d.replace("$", "")
                    d = d.replace("M", "")
                e = self.checking(e)
                f = self.checking(f)

            except Exception as ex:
                template = "An exception of type {0} occurred. Arguments:\n{1!r}"
                message = template.format(type(ex).__name__, ex.args)
                print (message)
                print (a)

            sql = 'INSERT INTO blockbuster_predicter(title ,genre, rating, gross, votes, movie_info_url) VALUES ("%s", "%s", "%s", "%s", "%s", "%s")' % (a, b, c, d, e, f)

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
        print (spider.movie_details['title'])
        print (spider.movie_details['movie_info_url'])
        print (spider.movie_details['genre'])
        print (spider.movie_details['rating'])
        print (spider.movie_details['gross'])
        print (spider.movie_details['votes'])


    def parse(self, response):

        if response.css('div.lister-list'):
            print ("Exist")
            print (response)

            for i in range(1,101):
                spider.movie_details['title'].append(response.css('div:nth-child('+ str(i) +') > div.lister-item-content > h3.lister-item-header > a ::text').extract_first())
                spider.movie_details['movie_info_url'].append(response.css('div:nth-child('+ str(i) +') > div.lister-item-content > h3.lister-item-header > a ::attr(href)').extract_first())
                spider.movie_details['genre'].append(response.css('div:nth-child('+ str(i) +') > div.lister-item-content > p.text-muted > span.genre ::text').extract_first())
                spider.movie_details['votes'].append(response.css('div:nth-child('+ str(i) +') > div.lister-item-content > p.sort-num_votes-visible > span:nth-child(2) ::text').extract_first())
                spider.movie_details['gross'].append(response.css('div:nth-child('+ str(i) +') > div.lister-item-content > p.sort-num_votes-visible > span:nth-child(5) ::text').extract_first())
                spider.movie_details['rating'].append(response.css('div:nth-child('+ str(i) +') > div.lister-item-content > div.ratings-bar > div.inline-block.ratings-imdb-rating > strong ::text').extract_first())
                i += 1

            spider.page_counter += 1
            print (spider.page_counter)
            if spider.page_counter <= 500:
                next_page_url = response.css("a.lister-page-next.next-page ::attr(href)").extract_first()
                print ("PAGE: ",spider.page_counter)
                print ("http://www.imdb.com/search/title" + next_page_url)
                yield response.follow("http://www.imdb.com/search/title" + next_page_url, self.parse)

        else:
            print ("All Records Read")

if __name__ == "__main__":
    spider = ToScrapeCSSSpider()
    spider.start_urls.append("http://www.imdb.com/search/title?adult=include&count=100&title_type=feature,tv_movie,documentary,short&page=101&ref_=adv_nxt")
    print (spider.start_urls)

    process = CrawlerProcess({'USER_AGENT': 'Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)'})
    process.crawl(ToScrapeCSSSpider)
    process.start()

    spider.display()
    spider.__insert_into_tabel__()
