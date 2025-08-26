import scrapy

class ExampleSpider(scrapy.Spider):
    name = "example_spider"
    start_urls = ["https://httpbin.org/html"]

    def parse(self, response):
        yield {
            "title": response.css("h1::text").get(),
            "url": response.url,
        }
