import re
import scrapy
import os
from scrapy.crawler import CrawlerProcess
import requests
from bs4 import BeautifulSoup
import csv


def get_products(link):
    page = requests.get(link)
    products = []
    print("Getting products from : ", link)
    if page.status_code == 200:
        data = BeautifulSoup(page.content, "html.parser")
        products = data.find_all("div", class_ = "sb-col l3 m3 sb-mobile sb-center sb-hover-opacity sb-border-bottom")
        products = [item.find('a').get("href") for item in products if item.find('a')]
    
    if len(products) == 0:
        products = data.find_all('div', class_ = "ut-animated-image-item ut-image-gallery-item ut-animation-done")
        products = [item.find('a').get("href") for item in products if item.find('a')]
        

    if len(products) == 0:
            products = data.find_all('div', class_ = "sb-col l3 m3 sb-mobile sb-center sb-hover-opacity")
            products = [item.find('a').get("href") for item in products if item.find('a')]
        
    print("Products found : ", len(products))
    return products




def get_products_links():
    url =  "https://smithbrothersfurniture.com/occasional-chairs/"
    req = requests.get(url)
    data = []
    if req.status_code == 200:
        soup = BeautifulSoup(req.content, "html.parser")
        menu = soup.find("ul", class_="sub-menu")
        categories = menu.find_all("a")
        categories = [[item.text.strip(), item.get("href")] for item in categories]
        
        for item in categories:
            category1 = item[0]
            if category1 != "BUILD YOUR OWN":
                products = get_products(item[1])
                for prod_link in products:
                    if prod_link.startswith("/style-details/"):
                        prod_link = "https://smithbrothersfurniture.com" + prod_link
                    row = [category1, prod_link]
                    data.append(row)

    csv_filename = "utilities/products-links.csv"
    with open(csv_filename, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["category1", "product_link"])  # Header
        writer.writerows(data)

    print(f"Data saved to {csv_filename}")
        

# --------------------------------------------------------------------------------------------------------------------------------------------

class ProductSpider(scrapy.Spider):
    name = "product_spider"
    custom_settings = {
        'DUPEFILTER_CLASS': 'scrapy.dupefilters.BaseDupeFilter',
        'CONCURRENT_REQUESTS': 1,
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

    columns = [
        "SKU", "START_DATE", "END_DATE", "DATE_QUALIFIER", "DISCONTINUED", "BRAND", "PRODUCT_GROUP1",
        "PRODUCT_GROUP2", "PRODUCT_GROUP3", "PRODUCT_GROUP4", "PRODUCT_GROUP1_QTY", "PRODUCT_GROUP2_QTY",
        "PRODUCT_GROUP3_QTY", "PRODUCT_GROUP4_QTY", "DEPARTMENT1", "ROOM1", "ROOM2", "ROOM3", "ROOM4",
        "ROOM5", "ROOM6", "CATEGORY1", "CATEGORY2", "CATEGORY3", "CATEGORY4", "CATEGORY5", "CATEGORY6",
        "COLLECTION", "FINISH1", "FINISH2", "FINISH3", "MATERIAL", "MOTION_TYPE1", "MOTION_TYPE2",
        "SECTIONAL", "TYPE1", "SUBTYPE1A", "SUBTYPE1B", "TYPE2", "SUBTYPE2A", "SUBTYPE2B",
        "TYPE3", "SUBTYPE3A", "SUBTYPE3B", "STYLE", "SUITE", "COUNTRY_OF_ORIGIN", "MADE_IN_USA",
        "BED_SIZE1", "FEATURES1", "TABLE_TYPE", "SEAT_TYPE", "WIDTH", "DEPTH", "HEIGHT", "LENGTH",
        "INSIDE_WIDTH", "INSIDE_DEPTH", "INSIDE_HEIGHT", "WEIGHT", "VOLUME", "DIAMETER", "ARM_HEIGHT",
        "SEAT_DEPTH", "SEAT_HEIGHT", "SEAT_WIDTH", "HEADBOARD_HEIGHT", "FOOTBOARD_HEIGHT",
        "NUMBER_OF_DRAWERS", "NUMBER_OF_LEAVES", "NUMBER_OF_SHELVES", "CARTON_WIDTH", "CARTON_DEPTH",
        "CARTON_HEIGHT", "CARTON_WEIGHT", "CARTON_VOLUME", "CARTON_LENGTH", "PHOTO1", "PHOTO2",
        "PHOTO3", "PHOTO4", "PHOTO5", "PHOTO6", "PHOTO7", "PHOTO8", "PHOTO9", "PHOTO10", "INFO1",
        "INFO2", "INFO3", "INFO4", "INFO5", "DESCRIPTION", "PRODUCT_DESCRIPTION",
        "SPECIFICATIONS", "CONSTRUCTION", "COLLECTION_FEATURES", "WARRANTY", "ADDITIONAL_INFORMATION",
        "DISCLAIMER", "VIEWTYPE", "ITEM_URL", "CATALOG_PDF"
    ]

    def __init__(self, input_file='utilities/products-links.csv', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.input_file = input_file
        os.makedirs('output', exist_ok=True)
        self.smith_brothers_file = open('output/smith_brothers.csv', 'w', newline='', encoding='utf-8')

        self.smith_brothers_writer = csv.DictWriter(self.smith_brothers_file, fieldnames=self.columns)

        self.smith_brothers_writer.writeheader()


    def start_requests(self):
        self.logger.info("Spider started. Reading product links from CSV file.")
        with open(self.input_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                yield scrapy.Request(
                    url=row['product_link'],
                    callback=self.parse,
                    meta={
                        'product_link': row['product_link'],
                        "category1" : row["category1"]
                    }
                )
    def parse(self, response):
        self.logger.info(f"Parsing product: {response.url}")
        sku = ""
        width = ""
        depth = ""
        height = ""
        description = ""
        collection = ''
        products_images = []
        specifications = {}
        specifications_str = ""
        catalog_pdf = ""
        page_title = ""

        try:
            meta = response.meta
            soup = BeautifulSoup(response.text, 'html.parser')
            data = {col: "" for col in self.columns} 

            try:
                sku = soup.find("span", class_ = "sb-style-no")
                if sku:
                    sku = sku.text.strip()
                else:
                    sku = ""
            except Exception as e:
                print("An error occurred while extracting SKU:", str(e))

            if sku != "":
                try:
                    if sku:
                        collection = sku + " " + "Style"
                    else:
                        collection = ""
                except Exception as e:
                    print("An error occurred while extracting Collection:", str(e))

                try:
                    description = soup.find("p", class_ ="sb-style-desc")
                    if description:
                        description = description.text.strip()
                    else:
                        description = ""
                except Exception as e:
                    print("An error occurred while extracting Description:", str(e))

                try:
                    catalog_pdf = soup.find("div", class_ = "sb-center sb-large")
                    if catalog_pdf:
                        catalog_pdf = catalog_pdf.find("a").get("href")
                    else:
                        catalog_pdf = ""
                except Exception as e:
                    print("An error occurred while extracting Catalog PDF:", str(e))

                try:
                    main_img = soup.find('img', class_ = "sb-style-details-image")
                    if main_img:
                        main_img = main_img.get("src")
                    else:
                        main_img = ""

                    products_images = soup.find_all("img", class_ = "sb-image sb-hover-opacity")
                    if products_images:
                        products_images = [item.get("src") for item in products_images]
                    else:
                        products_images = []

                    if len(products_images) ==0:
                        products_images = soup.find_all("img", class_ = "sb-image sb-hover-opacity sb-wide-25 sb-margin-right")
                        if products_images:
                            products_images = [item.get("src") for item in products_images]
                        else:
                            products_images = []

                    if main_img:
                        products_images.append(main_img)
                except Exception as e:
                    print("An error occurred while extracting Images:", str(e))

                try:
                    
                    specification_div = soup.find_all("div", class_ = "sb-row sb-mobile sb-margin-left")
                    if specification_div:
                        for item in specification_div:
                            divs = item.find_all("div")
                            if len(divs) == 2:
                                item_key = divs[0].find("strong")
                                if item_key:
                                    item_key = item_key.text.strip().replace(":", "")
                                else:
                                    continue
                                item_value = divs[1].find("span")
                                if item_value:
                                    item_value = item_value.text.strip().replace('"', "")
                                else:
                                    continue

                                if item_key and item_value:
                                    specifications[item_key] = item_value

                    if specifications:
                        width = specifications.get("Width", None)
                        depth = specifications.get("Depth", None)
                        height = specifications.get("Height", None)
                        specifications_str = "; ".join([f"{k}: {v}" for k, v in specifications.items()])
                    else:
                        width = ""
                        depth = ""
                        height = ""
                        specifications_str = ""
                except Exception as e:
                    print("An error occurred while extracting Specifications:", str(e))

                formatting_sku_for_uniqueness = str(main_img.split("/")[-1].split(".")[0]).upper() + "-"+ meta['category1'].replace(" ", "-")
                self.logger.info(formatting_sku_for_uniqueness)
                data.update({
                    "CATEGORY1": meta['category1'],
                    "COLLECTION": collection,
                    "ITEM_URL": meta['product_link'],
                    "SKU": formatting_sku_for_uniqueness,
                    "DESCRIPTION": description,
                    "PRODUCT_DESCRIPTION": "",
                    "WIDTH": width,
                    "DEPTH": depth,
                    "HEIGHT": height,
                    "ADDITIONAL_INFORMATION": "",
                    "FINISH1": "",
                    "CONSTRUCTION": "", 
                    "SPECIFICATIONS": specifications_str,
                    "BRAND": "Smith Brothers Furniture",
                    "VIEWTYPE": "Normal",
                    "CATALOG_PDF" : catalog_pdf,   
                })

                    
                for i in range(1, 11):
                    data[f"PHOTO{i}"] = ""
                for idx, img_url in enumerate(products_images):
                    if idx > 9:
                        continue
                    else:
                        data[f"PHOTO{idx + 1}"] = img_url
                if len(products_images) < 10:
                    self.logger.info(f"Only {len(products_images)} images found for {meta['product_link']}. Remaining PHOTO columns will be ''.")
                elif len(products_images) > 10:
                    self.logger.warning(f"More than 10 images found for {meta['product_link']}. Only the first 10 will be saved.")

                if len(products_images) == 0:
                    data.update({
                        "VIEWTYPE": "Limited",
                    })

                if sku:
                    self.smith_brothers_writer.writerow(data)
                    self.logger.info(f"Successfully scraped and categorized product: {meta['product_link']}")

            else:

                pass


        except Exception as e:
            self.logger.error(f"Error parsing product: {response.url}, {e}")

    def closed(self, reason):
        self.smith_brothers_file.close()
        self.logger.info("Spider closed. Files saved.")
 


# ----------------------------------------    RUN THE CODE   --------------------------------------------------------------------------------------------------
if __name__ == "__main__":

    # output_dir = 'utilities'
    # os.makedirs(output_dir, exist_ok=True)
    # get_products_links()
    process = CrawlerProcess()
    process.crawl(ProductSpider)
    process.start()