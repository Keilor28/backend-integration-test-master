import scrapy
import json
from scrapy import Request
from scrapers.items import ProductItem
from scrapy.exceptions import CloseSpider


class CaWalmartSpider(scrapy.Spider):
    name = "ca_walmart"
    allowed_domains = ["walmart.ca"]
    start_urls = ["https://www.walmart.ca/en/grocery/fruits-vegetables/fruits/N-3852"]
    item_count = 0
    walmart_url = 'https://www.walmart.ca'

#delay:90s
    def parse(self, response):
        list_links_fruits = response.css('a.product-link::attr("href")').getall()
        for product in list_links_fruits:
            url_product = self.walmart_url + product
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

        product_upc = json_product['entities']['skus'][sku]['upc'][0]
        branch_url = '/api/product-page/find-in-store?latitude=48.4120872&longitude=-89.2413988&lang=en&upc='
        url_api_product = self.walmart_url + branch_url + product_upc

        yield Request(url_api_product, callback=self.detail_branch, cb_kwargs={'item': item})

    def detail_branch(self, response, item):
        json_branch = json.loads(response.text)
        item['branch'] = json_branch['info'][0]['id']
        item['stock'] = json_branch['info'][0]['availableToSellQty']
        item['price'] = json_branch['info'][0]['sellPrice']

        yield item
