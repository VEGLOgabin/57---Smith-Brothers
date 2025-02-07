# 57---Smith-Brothers


I want to load this csv file:

SKU,START_DATE,END_DATE,DATE_QUALIFIER,DISCONTINUED,BRAND,PRODUCT_GROUP1,PRODUCT_GROUP2,PRODUCT_GROUP3,PRODUCT_GROUP4,PRODUCT_GROUP1_QTY,PRODUCT_GROUP2_QTY,PRODUCT_GROUP3_QTY,PRODUCT_GROUP4_QTY,DEPARTMENT1,ROOM1,ROOM2,ROOM3,ROOM4,ROOM5,ROOM6,CATEGORY1,CATEGORY2,CATEGORY3,CATEGORY4,CATEGORY5,CATEGORY6,COLLECTION,FINISH1,FINISH2,FINISH3,MATERIAL,MOTION_TYPE1,MOTION_TYPE2,SECTIONAL,TYPE1,SUBTYPE1A,SUBTYPE1B,TYPE2,SUBTYPE2A,SUBTYPE2B,TYPE3,SUBTYPE3A,SUBTYPE3B,STYLE,SUITE,COUNTRY_OF_ORIGIN,MADE_IN_USA,BED_SIZE1,FEATURES1,TABLE_TYPE,SEAT_TYPE,WIDTH,DEPTH,HEIGHT,LENGTH,INSIDE_WIDTH,INSIDE_DEPTH,INSIDE_HEIGHT,WEIGHT,VOLUME,DIAMETER,ARM_HEIGHT,SEAT_DEPTH,SEAT_HEIGHT,SEAT_WIDTH,HEADBOARD_HEIGHT,FOOTBOARD_HEIGHT,NUMBER_OF_DRAWERS,NUMBER_OF_LEAVES,NUMBER_OF_SHELVES,CARTON_WIDTH,CARTON_DEPTH,CARTON_HEIGHT,CARTON_WEIGHT,CARTON_VOLUME,CARTON_LENGTH,PHOTO1,PHOTO2,PHOTO3,PHOTO4,PHOTO5,PHOTO6,PHOTO7,PHOTO8,PHOTO9,PHOTO10,INFO1,INFO2,INFO3,INFO4,INFO5,DESCRIPTION,PRODUCT_DESCRIPTION,SPECIFICATIONS,CONSTRUCTION,COLLECTION_FEATURES,WARRANTY,ADDITIONAL_INFORMATION,DISCLAIMER,VIEWTYPE,ITEM_URL,CATALOG_PDF
211,,,,,Smith Brothers Furniture,,,,,,,,,,,,,,,,NEW STYLES,,,,,,211 Style,,,,,,,,,,,,,,,,,,,,,,,,,80,39,37,,,,,,,,,,,,,,,,,,,,,,,https://smithbrothersfurniture.com/ImgGlobal/Styles/public/211-sm-fabric-chair.jpg,https://smithbrothersfurniture.com/ImgGlobal/Styles/public/211-sm-fabric-ottoman.jpg,https://smithbrothersfurniture.com/ImgGlobal/Styles/public/211-sm-leather-sofa.jpg,https://smithbrothersfurniture.com/ImgGlobal/Styles/public/211-sm-leather-chair.jpg,https://smithbrothersfurniture.com/ImgGlobal/Styles/public/211-sm-leather-ottoman.jpg,https://smithbrothersfurniture.com/ImgGlobal/Styles/public/211-HD-fabric-sofa.jpg ,,,,,,,,,,Sofa,,Width: 80; Depth: 39; Height: 37; Inside Width: 66; Seat Height: 20; Seat Depth: 21; Arm Height: 26,,,,,,Normal,https://smithbrothersfurniture.com/style-details/?sid=211&piece=1,https://smithbrothersfurniture.com/downloads/CatalogPages/211.pdf


and so get  data from the catalog_pdf link , After that iwant to get all PHOTOX  column data which is not empty and work on them before save again the data to a new csv file , so after processing each row , all in a python class with different methods, one method will  do this:

import fitz

# Open the PDF file
pdf_path = "M100.pdf"  # Change this to the correct path
doc = fitz.open(pdf_path)

for page in doc:
    text = page.get_text("text")  # Extract text from page
    lines = text.split("\n")  # Split text into lines

    table_data = []
    extracted_text = []
    features = []
    is_table = False

    for line in lines:
        line = line.strip()

        # Detect the start of the table using regex
        if re.match(r"^[A-Z]?\d{3,}-\d{2,}\*?$", line.strip()):
            is_table = True

        if is_table:
            # Stop collecting lines when "*Battery Pack Available" is found
            if "*Battery Pack Available" in line:
                break  
            if line:  # Avoid empty lines
                table_data.append(line)
            # Stop when a blank line appears after collecting table data
            elif table_data:
                break 
        else:
            if "DIMENSIONS:" in line:
                features = extracted_text
            extracted_text.append(line) 
            if "FEATURES:" in line:
                extracted_text = []
  

    break  # Stop after extracting the first page

# Clean and format the table data
formatted_table = "\n".join(table_data)



print("---- Text Before First Table ----")
features_filtered = []
for item in features:
    if "DIMENSIONS:" in item:
        break
    features_filtered.append(item)
print("\n".join(features_filtered))

print("\n---- First Table Data ----")
print("\n".join(table_data))


another method will get all PHOTO1, PHOTO2,... which is not empty,   after that another function will save the data to a new csv file with the save columns as the current one but just with different sku, differents PHOTOX, and with CONSTRUCTION COLUMN equal to features_str