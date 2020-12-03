import scrapy
import json
from scrapy import Request
from scrapers.items import ProductItem
from scrapy.exceptions import CloseSpider


class CaWalmartSpider(scrapy.Spider):
    name = "ca_walmart"
    allowed_domains = ["www.walmart.ca"]
    start_urls = ["https://www.walmart.ca/en/grocery/fruits-vegetables/fruits/N-3852"]


def parse(self, response):
        list_links_fruits = response.css('a.product-link::attr("href")').getall()
        for product in list_links_fruits:
            url_product = self.base_walmart_url + product
            yield Request(url=url_product, callback=self.product_detail, cb_kwargs={'url': url_product})

        next_page = response.css('#loadmore::attr(href)').get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)
        
