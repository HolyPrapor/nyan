import json
import shutil
from datetime import datetime, timezone, timedelta

import scrapy
import feedparser

def to_timestamp(dt_str):
    dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S+00:00")
    dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())

def get_current_ts():
    return int(datetime.now().replace(tzinfo=timezone.utc).timestamp())

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
                post_ts = to_timestamp(entry.published_parsed)
                if post_ts < self.until_ts:
                    continue
                content = entry.get('content')
                if content:
                    content = content[0]['value']

                item = {
                    'feed_title': feed_title,
                    'link': entry.link,
                    'title': entry.title,
                    'description': entry.description,
                    "pub_time": entry.published_parsed,
                    "fetch_time": get_current_ts(),
                    'text': content,
                    'type': entry.get('dc_type'),
                    "views": 0
                }

                yield item

    def closed(self, reason):
        temp_path = self.fetch_times_path + ".new"
        with open(temp_path, "w") as w:
            json.dump(self.fetch_times, w)
        shutil.move(temp_path, self.fetch_times_path)
