import re
import scrapy
import os
from scrapy.crawler import CrawlerProcess
import requests
from bs4 import BeautifulSoup
import csv
import pandas as pd
import fitz
import io

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

                data.update({
                    "CATEGORY1": meta['category1'],
                    "COLLECTION": collection,
                    "ITEM_URL": meta['product_link'],
                    "SKU": sku,
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
 

# -----------------------------   PDF Data Processing  -------------------------------------

class CatalogProcessor:
    def __init__(self, csv_file, output_file):
        self.csv_file = csv_file
        self.output_file = output_file
        self.data = None

    def load_csv(self):
        """Load the CSV file into a Pandas DataFrame."""
        self.data = pd.read_csv(self.csv_file, dtype = str).fillna("")

    def process_table_data(self, data):
        result = []
        current_entry = []

        for item in data:
            parts = item.split("-")
            if len(parts) == 2 and parts[0] != "" and (any(c.isdigit() for c in parts[0]) or any(c.isdigit() for c in parts[1])):
                if current_entry:
                    result.append(current_entry)
                current_entry = [item]
            elif (item[0].isdigit() and len(item)<3) or item == '-':  
                current_entry.append(item)
            else:
                if len(current_entry) == 1:
                    current_entry.append(item)
                else:
                    if len(current_entry)==2:
                        current_entry[1] += " " + item 

        if current_entry:
            result.append(current_entry) 

        return result
    
    def extract_pdf_data(self, pdf_url):
        """Extract table data from the given PDF URL."""
        try:
            response = requests.get(pdf_url)
            response.raise_for_status()
            doc = fitz.open(stream=io.BytesIO(response.content), filetype="pdf")

            for page in doc:
                text = page.get_text("text")
                lines = text.split("\n")

                table_data = []
                is_table = False
                features = []
                extracted_text = []

                for line in lines:
                    line = line.strip()
                    if re.match(r"^[A-Z]?\d{3,}-\d{2,}\*?[A-Z]*$", line.strip()):
                        is_table = True

                    if is_table:
                        if "*Battery Pack Available" in line:
                            break
                        if line:
                            table_data.append(line)
                        elif table_data:
                            break  
                    else:
                        if "DIMENSIONS:" in line:
                            features = extracted_text
                        extracted_text.append(line) 
                        if "FEATURES:" in line:
                            extracted_text = []

                features_filtered = []
                for item in features:
                    if "DIMENSIONS:" in item:
                        break
                    features_filtered.append(item)
                

                return {
                    "features_data" : ",".join(features_filtered),
                    "dimensions_table": self.process_table_data(table_data)
                }
            
        except requests.RequestException as e:
            print(f"Failed to download PDF: {e}")
            return ""
    
    def extract_photos(self, row):
        """Extract all non-empty PHOTO columns from a row."""
        return [row[f"PHOTO{i}"] for i in range(1, 11) if row.get(f"PHOTO{i}", "").strip()]
    
    def process_data(self):
        """Process each row to extract PDF data and photos, then save the result."""
        new_rows = []
        for _, row in self.data.iterrows():
            if row["CATALOG_PDF"]:
                pdf_data =  self.extract_pdf_data(row["CATALOG_PDF"])
                construction_text =  pdf_data["features_data"]
                dimentions_table = pdf_data["dimensions_table"]
                products_titles = [item[1] for item in dimentions_table if dimentions_table]
                photos = self.extract_photos(row)

                for prod_dim in dimentions_table:
                    print("****************************************************************************************************************")
                    print(prod_dim)
                    products_images = []
                    pattern_search = ""
                    pattern_exist_in_title = False
                    for img in photos:
                        if "-fabric-" in img:
                            pattern_search = img.split('-fabric-')[-1].replace(".jpg", "")
                            if "-" in pattern_search:
                                pattern_search = pattern_search.split("-")
                                pattern_search = [item.title() for item in pattern_search]
                                for item_search in pattern_search:
                                    if item_search in prod_dim[1]:
                                        products_images.append(img.replace("-sm-", "-HD-"))
                                        pattern_exist_in_title = True
                                    else:
                                        for title in products_titles:
                                            if item_search in title:
                                                pattern_exist_in_title = True    

                            else:
                                pattern_search = pattern_search.title()
                                if pattern_search in prod_dim[1]:
                                    products_images.append(img.replace("-sm-", "-HD-"))
                                    pattern_exist_in_title = True
                                else:
                                    for title in products_titles:
                                        if pattern_search in title:
                                            pattern_exist_in_title = True

                        if "-leather-" in img:
                            pattern_search = img.split('-leather-')[-1].replace(".jpg", "")
                            if "-" in pattern_search:
                                pattern_search = pattern_search.split("-")
                                pattern_search = [item.title() for item in pattern_search]
                                for item_search in pattern_search:
                                    if item_search in prod_dim[1]:
                                        products_images.append(img.replace("-sm-", "-HD-"))
                                        pattern_exist_in_title = True
                                    else:
                                        for title in products_titles:
                                            if item_search in title:
                                                pattern_exist_in_title = True

                            else:
                                pattern_search = pattern_search.title()
                                if pattern_search in prod_dim[1]:
                                    products_images.append(img.replace("-sm-", "-HD-"))
                                    pattern_exist_in_title = True
                                else:
                                    for title in products_titles:
                                        if pattern_search in title:
                                            pattern_exist_in_title = True


                        if not pattern_exist_in_title:
                            products_images.append(img.replace("-sm-", "-HD-"))
                    new_row = row.copy()

                    if len(prod_dim)>=5:
                        width = prod_dim[2]
                        depth = prod_dim[3]
                        height = prod_dim[4]
                        new_row.update({
                        "WIDTH": width,
                        "DEPTH": depth,
                        "HEIGHT": height,
                        })

                    new_row.update({
                        "SKU": prod_dim[0].replace("*", ""),
                        "DESCRIPTION": prod_dim[1],
                        "CONSTRUCTION": construction_text, 
                        })
                        

                    for i in range(1, 11):
                        new_row[f"PHOTO{i}"] = ""
                    for idx, img_url in enumerate(products_images):
                        if idx > 9:
                            continue
                        else:
                            new_row[f"PHOTO{idx + 1}"] = img_url
                    if len(products_images) < 10:
                        print(f"Only {len(products_images)} images found for -------- {prod_dim[0]} --------. Remaining PHOTO columns will be ''.")
                    elif len(products_images) > 10:
                        print(f"More than 10 images found for ---------- {prod_dim[0]} -----------. Only the first 10 will be saved.")

                    if len(products_images) == 0:
                        new_row.update({
                            "VIEWTYPE": "Limited",
                        })

                    new_rows.append(new_row)
        new_df = pd.DataFrame(new_rows)
        new_df.to_csv(self.output_file, index=False)






# ----------------------------------------    RUN THE CODE   --------------------------------------------------------------------------------------------------
if __name__ == "__main__":

    output_dir = 'utilities'
    os.makedirs(output_dir, exist_ok=True)
    get_products_links()
    process = CrawlerProcess()
    process.crawl(ProductSpider)
    process.start()
    processor = CatalogProcessor("output/smith_brothers.csv", "output/smith_brothers_sku_updated.csv")
    processor.load_csv()
    processor.process_data()