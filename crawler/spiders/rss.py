import json
import shutil
from datetime import datetime, timezone, timedelta
from bs4 import BeautifulSoup
from googletrans import Translator
import scrapy
import feedparser
import time


def convert_to_timestamp(published_parsed):
    return int(time.mktime(published_parsed))


def get_current_ts():
    return int(datetime.now().replace(tzinfo=timezone.utc).timestamp())


def strip_html_tags(text):
    return BeautifulSoup(text, "html.parser").get_text(separator=' ', strip=True)


class RSSSpider(scrapy.Spider):
    name = "rss"

    def __init__(self, *args, **kwargs):
        assert "feeds_file" in kwargs
        with open(kwargs.pop("feeds_file")) as r:
            self.feeds = json.load(r)["feeds"]
            self.feeds = {feed["name"]: feed for feed in self.feeds}
        assert "fetch_times" in kwargs
        self.fetch_times_path = kwargs.pop("fetch_times")
        with open(self.fetch_times_path) as r:
            self.fetch_times = json.load(r)

        assert "hours" in kwargs
        hours = int(kwargs.pop("hours"))
        self.until_ts = int((datetime.now() - timedelta(hours=hours)).timestamp())
        print("Considering last {} hours".format(hours))

        self.translator = Translator()

        super().__init__(*args, **kwargs)

    def start_requests(self):
        feeds = self.feeds
        feeds = [feed for feed in feeds.values() if not feed.get("disabled", False)]
        urls = []
        current_ts = get_current_ts()
        for feed in feeds:
            last_fetch_time = self.fetch_times.get(feed, 0)
            recrawl_time = feed.get("recrawl_time", 0)
            assert current_ts >= last_fetch_time
            if current_ts - last_fetch_time < recrawl_time:
                print("Skip {}, current ts: {}, last fetch ts: {}, recrawl interval: {}".format(feed["name"], current_ts, last_fetch_time, recrawl_time))
                continue
            urls.append(scrapy.Request(feed["url"]))
        return urls

    def parse_feed(self, feed):
        data = feedparser.parse(feed)
        if data.bozo:
            print('Bozo feed data. %s: %r' % (data.bozo_exception.__class__.__name__, data.bozo_exception))
            if (hasattr(data.bozo_exception, 'getLineNumber') and
                    hasattr(data.bozo_exception, 'getMessage')):
                print('Line %d: %s' % (data.bozo_exception.getLineNumber(), data.bozo_exception.getMessage()))
            return None
        self.fetch_times[feed["name"]] = get_current_ts()
        return data

    def parse(self, response):
        feed = self.parse_feed(response.body)
        if feed:
            feed_title = feed.feed.title

            for entry in feed.entries:
                post_ts = convert_to_timestamp(entry.published_parsed)
                if post_ts < self.until_ts:
                    continue
                content = entry.get('content')
                if content:
                    content = content[0]['value']

                title = strip_html_tags(entry.title)
                description = strip_html_tags(entry.description)
                content = strip_html_tags(content) if content else ""
                translated_title = self.translator.translate(entry.title, dest='ru', src='de').text
                translated_description = self.translator.translate(entry.description, dest='ru', src='de').text
                translated_content = self.translator.translate(content, dest='ru', src='de').text

                item = {
                    'feed_title': feed_title,
                    'link': entry.link,
                    'src_title': title,
                    "title": translated_title,
                    'src_description': description,
                    'description': translated_description,
                    "pub_time": post_ts,
                    "fetch_time": get_current_ts(),
                    "src_content": content,
                    'text': translated_content or translated_description or translated_title,
                    "views": 0
                }

                yield item

    def closed(self, reason):
        temp_path = self.fetch_times_path + ".new"
        with open(temp_path, "w") as w:
            json.dump(self.fetch_times, w)
        shutil.move(temp_path, self.fetch_times_path)
