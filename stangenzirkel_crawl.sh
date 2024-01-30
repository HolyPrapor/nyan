while :
do
    scrapy crawl rss -a channels_file=stangenzirkel_channels.json -a fetch_times=crawler/fetch_times.json -a hours=24
done
