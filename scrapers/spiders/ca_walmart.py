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
            yield Request(url=url_product, callback=self.detail, cb_kwargs={'url': url_product})

        next_page = response.css('#loadmore::attr(href)').get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)


def detail(self, response, url):
        script = response.xpath('/html/body/script[1]/text()').get()
        product = script.replace('window.__PRELOADED_STATE__=', '')[:-1]
        json_product = json.loads(product)
        sku = json_product['product']['activeSkuId']

        item = ProductItem()
        item['store'] = response.xpath('/html/head/meta[10]/@content').get()
        item['barcodes'] = ' '.join(json_product['entities']['skus'][sku]['upc'])
        item['image_url'] = json_product['entities']['skus'][sku]['images'][0]['large']['url']
        item['sku'] = json_product['product']['activeSkuId']
        item['brand'] = json_product['entities']['skus'][sku]['brand']['name']
        item['name'] = json_product['entities']['skus'][sku]['name']
        item['description'] = json_product['entities']['skus'][sku]['longDescription']
        item['package'] = json_product['product']['item']['description']
        item['url'] = url
        item['category'] = json_product['entities']['skus'][sku]['categories'][0]['displayName']


        yield item

