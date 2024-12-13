from twisted.internet import asyncioreactor
asyncioreactor.install()
from scrapy_playwright.page import PageMethod
import json
import scrapy
import os
from scrapy.crawler import CrawlerProcess
import requests
from bs4 import BeautifulSoup
from playwright.async_api import TimeoutError


import json
import urllib.parse

class CategoryProductScraper:
    
    def __init__(self):
        
        self.file_path = "utilities/urls.json"
        self.headers = {
                        'accept': '*/*',
                        'accept-language': 'en-US,en;q=0.9',
                        'origin': 'https://www.visualcomfort.com',
                        'priority': 'u=1, i',
                        'referer': 'https://www.visualcomfort.com/',
                        'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
                        'sec-ch-ua-mobile': '?0',
                        'sec-ch-ua-platform': '"Linux"',
                        'sec-fetch-dest': 'empty',
                        'sec-fetch-mode': 'cors',
                        'sec-fetch-site': 'cross-site',
                        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
                        }
        
        self.data=[]
        
    
    
    def load_urls_from_json(self):
        with open(self.file_path, 'r') as file:
            return json.load(file)

    def update_url_parameters(self, url, start_value, rows_value):
        parsed_url = urllib.parse.urlparse(url)
        query_params = urllib.parse.parse_qs(parsed_url.query)
        query_params['start'] = [str(start_value)]
        query_params['rows'] = [str(rows_value)]
        updated_query = urllib.parse.urlencode(query_params, doseq=True)
        updated_url = parsed_url._replace(query=updated_query).geturl()
        return updated_url
    
    
    
    def send_request_and_response(self, url):
        response = requests.get(
                                    url,
                                    headers=self.headers,
                                )
        return response.json()
    
    
    
    
    def get_numFound(self, url):
        
        response = self.send_request_and_response(url)
        
        if response:
            numFound = response["response"]['numFound']
            return numFound
        else:
            return None
        
        
    def save_products_links(self):
        output_dir = 'utilities'
        os.makedirs(output_dir, exist_ok=True)
        with open("utilities/products-links.json", 'w') as json_file:
            json.dump(self.data, json_file, indent=4)
        print(f"Data successfully saved to utilities/products-links.json")
    
    def extract_response(self, response, category_name, collection_name):
        if response:
            data = response["response"]
            data = data["docs"]

            for item in range(len(data)):
                product_data = data[item]

                product = {
                    "category_name": category_name,
                    "collection_name": collection_name,
                    'product_link': "https://www.visualcomfort.com" + product_data["url"],
                    'title': product_data.get('title', ''),
                    'brand': product_data.get('brand', ''),
                    'designer': product_data.get('designer', ''),
                    'thumb_image': product_data.get('thumb_image', ''),
                    'detail_description': product_data.get('detail_description', ''),
                    'series': product_data.get('series', ''),
                    'sku': product_data.get('pid', ''),
                    'variants': []
                }
                if "variants" in product_data:
                    for variant in product_data["variants"]:
                        imgs = variant.get('sku_swatch_images', [])
                        if imgs:
                            for idx, item in enumerate(imgs):
                                image = item.split("?")[0]
                                imgs[idx] = image
                                
                            
                        variant_info = {
                            'SKU': variant.get('skuid', ''),
                            'Image URL': imgs,
                            'Description': variant.get('detail_description', []),
                            'Badge': variant.get('badge', []),
                            'Series': variant.get('series', [])
                        }
                        product['variants'].append(variant_info)
                self.data.append(product)

            
            
            
    
    def get_category_products(self, url, numFound, category_name, collection_name):
        
        start = 0
        rows = 200
        
        while start < numFound:
            if start + rows > numFound:
                rows = numFound - start
                
            url_update = self.update_url_parameters(url, start, rows)
            response = self.send_request_and_response(url_update)
            self.extract_response(response, category_name, collection_name)
            
            start += rows
        
    
    
    def scrape_products_links(self):
        urls =  self.load_urls_from_json()
        for url_data in urls:
            url = url_data.get("category_link")
            category_name = url_data.get("category_name")
            collection_name = url_data.get("collection_name")
            print(f"{category_name} - {collection_name} products scraping .........")
            numFound = self.get_numFound(url)
            if numFound != None and numFound>0:
                self.get_category_products(url, numFound, category_name, collection_name)
            else:
                print(f'No product found for {url}')
                
        self.save_products_links()
                
        print(f"Length of the data : {len(self.data)}")
        
        
        
                



class ProductSpider(scrapy.Spider):
    name = "product_spider"
    
    custom_settings = {
        'DOWNLOAD_HANDLERS': {
            'http': 'scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler',
            'https': 'scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler',
        },
        'PLAYWRIGHT_LAUNCH_OPTIONS': {
            'headless': True,
            'timeout': 100000,
        },
        'DUPEFILTER_CLASS': 'scrapy.dupefilters.BaseDupeFilter',
        'CONCURRENT_REQUESTS': 1,
        # Disable default Scrapy's logging to reduce clutter, optional
        'LOG_LEVEL': 'INFO',
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 3,
        'RETRY_HTTP_CODES': [500, 502, 503, 504, 522, 524, 408, 429],
        'HTTPERROR_ALLOW_ALL': True,
        'DEFAULT_REQUEST_HEADERS': {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) ' \
                        'AppleWebKit/537.36 (KHTML, like Gecko) ' \
                        'Chrome/115.0.0.0 Safari/537.36',
            'Accept-Language': 'en',
        },
    }
    
    def start_requests(self):
        """Initial request handler."""
        self.logger.info("Spider started. Preparing to scrape products.")
        os.makedirs('output', exist_ok=True)
        self.scraped_data = []
        scraped_links = set()
        
        # Initialize JSON Lines file
        self.output_file = open('output/products-data.json', 'a', encoding='utf-8')
        
        # Load existing data if any
        if os.path.exists('output/products-data.json'):
            self.logger.info("Loading existing scraped data.")
            with open('output/products-data.json', 'r', encoding='utf-8') as f:
                
                    try:
                        self.scraped_data = json.load(f)
                        scraped_links = {(item['Product Link'], item["Collection"], item['Category']) for item in self.scraped_data}
                    except json.JSONDecodeError:
                        self.logger.warning("Encountered JSONDecodeError while loading existing data. Skipping line.")
                        pass 
        
        
        scraped_product_links = {item['Product Link'] for item in self.scraped_data}
        
        # Load products to scrape
        try:
            with open('utilities/products-links.json', 'r', encoding='utf-8') as file:
                products = json.load(file)
            self.logger.info(f"Loaded {len(products)} products to scrape.")
        except Exception as e:
            self.logger.error(f"Failed to load products-links.json: {e}")
            return
        
        for product in products:
            product_link = product['product_link']
            category_name = product['category_name']
            collection_name = product['collection_name']
            product_key = (product_link, collection_name, category_name)
            if product_key not in scraped_links:
                if product_link in scraped_product_links:
                    scraped_product = next((item for item in self.scraped_data if item['Product Link'] == product_link), None)
                    if scraped_product:
                        if collection_name not in scraped_product['Collection'] or category_name not in scraped_product['Category']:
                            new_product_data = scraped_product.copy()
                            new_product_data['Collection'] = collection_name
                            new_product_data['Category'] = category_name
                            
                            self.scraped_data.append(new_product_data)
                            with open('output/products-data.json', 'w', encoding='utf-8') as f:
                                json.dump(self.scraped_data, f, ensure_ascii=False, indent=4)
                            self.logger.info(f"Updated product with new collection or category: {product_link}")
                    else:
                        self.logger.warning(f"Product link found in scraped_product_links but not in scraped_data: {product_link}")
                else:
                    yield scrapy.Request(
                        url=product_link,
                        meta={
                            'playwright': True,
                            'playwright_include_page': True,
                            'product': product
                        },
                        callback=self.parse,
                        errback=self.handle_error
                    )
            else:
                self.logger.info(f"Skipping already scraped product: {product_link} under category: {category_name}")
    
    async def parse(self, response):
        """Parse the product page using BeautifulSoup and extract details."""
        self.logger.info(f"Parsing product: {response.url}")
        try:
            product = response.meta['product']
            page = response.meta['playwright_page']
            await page.wait_for_selector('h1.page-title')  
            try:
                await page.wait_for_selector('img.fotorama__img', timeout=10000)
            except TimeoutError:
                self.logger.warning("Timeout reached while waiting for 'img.fotorama__img'. Continuing execution...")
                
            try:
                await page.wait_for_selector('.block.files-grid', timeout=10000)
            except TimeoutError:
                self.logger.warning("Timeout reached while waiting for '.block.files-grid'. Continuing execution...")
            
            content = await page.content()
            soup = BeautifulSoup(content, 'html.parser')
            product_images = []
            category_name = product['category_name']
            collection_name = product['collection_name']
            product_link = product['product_link']
            series = product['series']
            brand = product['brand']
            sku = product['sku']
            product_name = product['title']
            designer = product['designer']
            variants = product['variants']
            thumb_image = product.get("thumb_image", "")
            if thumb_image:
                thumb_image = thumb_image.split("?")[0]
                product_images.append(thumb_image)
            
            
            specifications = []
            for tab_id in ['#spec-inch-tab', '#spec-cm-tab']:
                rows = soup.select(f'{tab_id} .data-table tbody tr')
                for row in rows:
                    row_text = row.get_text(" ", strip=True) 
                    if row_text not in specifications:
                        specifications.append(row_text)
            additional_info = soup.select('table.options tbody tr')
            for row in additional_info:
                row_text = row.get_text(" ", strip=True)  
                if row_text not in specifications:
                    specifications.append(row_text)
                    
                    
            specifications_list = [item for item in specifications if item]
                
                    
            tech_resources = []
            resources_section = soup.find('div', class_='block files-grid')
            if resources_section:
                resource_items = resources_section.select('.tech-resource-item a')
                for item in resource_items:
                    resource_label = item.get_text(strip=True)
                    resource_url = item.get('href', None)  
                    if resource_url: 
                        tech_resources.append({
                            'Label': resource_label,
                            'URL': "https://www.visualcomfort.com" + resource_url
                        })
                    
            image_thumbs = soup.find_all("img", class_="fotorama__img")
            for img in image_thumbs:
                img_url = img.get('src', '')
                if '?' in img_url:
                    img_url = img_url.split('?')[0]
                if ".svg" in img_url:
                    continue
                product_images.append(img_url)
                
            product_images = list(set(product_images))
            video = soup.find("div", class_="product-video")
            
            if video:
                iframe = video.find("iframe")
                if iframe and iframe.get("src"):
                    video = "https:" + iframe.get("src")
                else:
                    video = ""
            else:
                video = ""
            
            description_div = soup.find('div', class_='additional-description')
            if description_div:
                content_div = description_div.find('div', class_='content')
                if content_div:
                    description = content_div.get_text(strip=True)
                else:
                    description = 'N/A'
            else:
                description = 'N/A'
            
            if not description or description == "N/A":
                description = product.get('detail_description', 'N/A')
            
            new_product_data = {
                'Category': category_name,
                'Collection': collection_name,
                'Product Link': product_link,
                'Product Title': product_name,
                'SKU': sku,
                "Series": series,
                "Designer": designer,
                "Brand": brand,
                "Video": video,
                'Technical Resources': tech_resources,
                'Product Images': product_images,
                "Product Specifications": specifications_list,
                "Description": description,
                "Variations": variants,
            }
            self.scraped_data.append(new_product_data)
            with open('output/products-data.json', 'w', encoding='utf-8') as f:
                json.dump(self.scraped_data, f, ensure_ascii=False, indent=4)
            self.logger.info(f"Successfully scraped product: {product_link}")
        
        
        except Exception as e:
            self.logger.error(f"Error parsing {response.url}: {e}")
        finally:
            await page.close()
    
    def handle_error(self, failure):
        self.logger.error(f"Request failed: {failure.request.url}")
        self.logger.error(repr(failure))
    
    def closed(self, reason):
        self.output_file.close()
        self.logger.info("Spider closed: %s", reason)
    
    
    
#   -----------------------------------------------------------Run------------------------------------------------------------------------


if __name__ == "__main__":
    output_dir = 'utilities'
    os.makedirs(output_dir, exist_ok=True)
    products_links_scraper = CategoryProductScraper()
    products_links_scraper.scrape_products_links()
    process = CrawlerProcess()
    process.crawl(ProductSpider)  
    process.start()