# -*- coding: utf-8 -*-
import scrapy
import re
from fang.items import XfItem,EsfItem
import time
from scrapy_redis.spiders import RedisSpider

class SfwSpider(RedisSpider):
    name = 'sfw'
    allowed_domains = ['fang.com']
    #start_urls = ['https://www.fang.com/SoufunFamily.htm']
    redis_key = "fang:start_urls"


    def parse(self, response):
        trs = response.xpath("//div[@class='outCont']//tr")
        province = None
        for tr in trs:
            tds = tr.xpath(".//td[not(@class)]")
            province_td = tds[0]
            province_text = province_td.xpath(".//text()").get()
            province_text = re.sub(r"\s","",province_text)
            if province_text:
                province = province_text

            city_tds = tds[1]
            city_links = city_tds.xpath(".//a")
            for city_link in city_links:
                city_text = city_link.xpath(".//text()").get()
                city_url = city_link.xpath(".//@href").get()
                # print("省份：", province)
                # print("城市：", city_text)
                # print("链接：", city_url)

                # 构建新房的url链接
                url_module = city_url.split(".")
                xf_url = url_module[0] + ".newhouse." + url_module[1] + "." + url_module[2] + "/house/s/"

                # 构建二手房的url链接
                esf_url = url_module[0] + ".esf." + url_module[1] + "." + url_module[2]

                yield scrapy.Request(url=xf_url,callback=self.parse_xf,meta={'info':(province,city_text)})
                time.sleep(0.8)
                yield scrapy.Request(url=esf_url,callback=self.parse_esf,meta={'info':(province,city_text)})


    def parse_xf(self,response):
        province,city_text = response.meta.get('info')
        lis = response.xpath("//div[contains(@class,'nl_con')]//li")
        for li in lis:
            name1 = li.xpath(".//div[contains(@class,'house_value')]//a/text()").get()
            if name1 is not None:
                name = name1.strip()
            rooms = li.xpath(".//div[contains(@class,'house_type')]/a/text()").getall()
            # 过滤出含居的字符
            rooms = list(filter(lambda x:x.endswith('居'),rooms))
            area = ''.join(li.xpath(".//div[contains(@class,'house_type')]/text()").getall()).strip()
            area = re.sub('/|－|\s','',area)
            address = li.xpath(".//div[@class='address']/a/@title").get()
            district_text = ''.join(li.xpath(".//div[@class='address']/a//text()").getall())
            district1 = re.sub(r"\s","",district_text)
            district = re.search(r"\[(.+)\]",district1)
            # 如果获取到的元素非空，再进行group操作
            if district is not None:
                district0 = district.group(1)
            # print(district0)
            sale = li.xpath(".//div[contains(@class,'fangyuan')]/span/text()").get()
            price = ''.join(li.xpath(".//div[@class='nhouse_price']//text()").getall()).strip()
            origin_url_text = li.xpath(".//div[@class='nlcd_name']/a/@href").get()
            if origin_url_text is not None:
                origin_url = "https:" + origin_url_text

            item = XfItem(name=name,rooms=rooms,area=area,address=address,district0=district0,sale=sale,price=price,origin_url=origin_url,province=province,city_text=city_text)
            # 给pipeline
            yield item
            next_url_text = response.xpath("//div[@class='page']//a[@class='next']/@href").get()
            next_url = response.urljoin(next_url_text)
            # 循环调用，爬取下一页
            if next_url:
                yield scrapy.Request(url = next_url,callback=self.parse_xf,meta={"info":(province,city_text)})




    def parse_esf(self,response):
        province, city_text = response.meta.get('info')

        dls = response.xpath("//div[contains(@class,'shop_list')]/dl")
        for dl in dls:
            item = EsfItem(province=province,city_text=city_text)
            item['name'] = dl.xpath(".//p[@class='add_shop']/a/@title").get()
            infos = dl.xpath(".//p[@class='tel_shop']/text()").getall()
            infos = list(map(lambda x:re.sub(r"\s","",x),infos))
            for info in infos:
                if "厅" in info:
                    item['rooms'] = info
                elif "㎡" in info:
                    item['area'] = info
                elif "层" in info:
                    item['floor'] = info
                elif "向" in info:
                    item['toward'] = info
            item['address'] = dl.xpath(".//p[@class='add_shop']/span/text()").get()
            price1 = ''.join(dl.xpath(".//dd[@class='price_right']/span/b/text()").getall())
            price2 = ''.join(dl.xpath(".//dd[@class='price_right']/span/text()").getall()).strip()
            price = price1 + price2
            item['price'] = price
            origin_url = dl.xpath(".//h4[@class='clearfix']/a/@href").get()
            origin_url = response.urljoin(origin_url)
            item['origin_url'] = origin_url
            yield item

        next_url_text = response.xpath("//div[@id='list_D10_15']/p[1]/a/@href").get()
        next_url = response.urljoin(next_url_text)
        if next_url:
            yield scrapy.Request(url=next_url,callback=self.parse_esf,meta={'info':(province,city_text)})


