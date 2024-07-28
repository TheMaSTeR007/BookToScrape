import gzip, hashlib, json, os, requests, re, pymysql
from lxml import html
from pymysql import cursors


def req_sender(url: str, method: str) -> bytes or None:
    # Prepare headers for the HTTP request
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:106.0) Gecko/20100101 Firefox/106.0',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-User': '?1',
    }
    # Send HTTP request
    _response = requests.request(method=method, url=url)
    # Check if response is successful
    if _response.status_code != 200:
        print(f"HTTP Status code: {_response.status_code}")  # Print status code if not 200
        return None
    return _response  # Return the response if successful


def product_data_scrape(this_page_product_links: list, this_category_products: list, method: str, directory_path: str, each_category_link: str, category_name: str, output: list):
    for this_product in this_page_product_links:
        print('in for ')
        # Store each product's information
        each_product_html = page_checker(url=this_product, method=method, directory_path=directory_path)
        parsed_each_product = html.fromstring(each_product_html)

        xpath_product_name = '//div[@class="col-sm-6 product_main"]/h1/text()'
        product_name = parsed_each_product.xpath(xpath_product_name)[0]

        xpath_product_price = '//div[@class="col-sm-6 product_main"]/p[@class="price_color"]/text()'
        product_price = float(parsed_each_product.xpath(xpath_product_price)[0][1:])
        currency_symbol = parsed_each_product.xpath(xpath_product_price)[0][0]

        xpath_product_in_stock = '//div[@class="col-sm-6 product_main"]/p[@class="instock availability"]/text()[last()]'
        product_stock = parsed_each_product.xpath(xpath_product_in_stock)[0]
        product_in_stock = True if product_stock.strip().startswith("In stock") else False

        product_quantity = int(re.search(r'(\d+)', product_stock).group())

        xpath_product_ratings = '//div[@class="col-sm-6 product_main"]/p[contains(@class, "star-rating")]/@class'
        ratings = parsed_each_product.xpath(xpath_product_ratings)[0].replace('star-rating ', "")
        rating_int = {'One': 1, 'Two': 2, 'Three': 3, 'Four': 4, 'Five': 5}
        product_ratings = rating_int.get(ratings, None)

        xpath_product_image_link = '//div[@class="item active"]/img/@src'
        product_image_link = 'https://books.toscrape.com/' + parsed_each_product.xpath(xpath_product_image_link)[0].replace('../../', '')

        xpath_product_description = '//div[@id="product_description"]/following-sibling::p/text()'
        product_description = parsed_each_product.xpath(xpath_product_description)[0] if parsed_each_product.xpath(xpath_product_description) else ''

        # UPC Text & Value
        xpath_product_info_UPC = '//table[@class="table table-striped"]//tr[1]/th/text()'
        product_info_UPC = parsed_each_product.xpath(xpath_product_info_UPC)[0]
        xpath_product_info_UPC_value = '//table[@class="table table-striped"]//tr[1]/td/text()'
        product_info_UPC_value = parsed_each_product.xpath(xpath_product_info_UPC_value)[0]

        # Product Type Text & Value
        xpath_product_info_ProdType = '//table[@class="table table-striped"]//tr[2]/th/text()'
        product_info_ProdType = parsed_each_product.xpath(xpath_product_info_ProdType)[0]
        xpath_product_info_ProdType_value = '//table[@class="table table-striped"]//tr[2]/td/text()'
        product_info_ProdType_value = parsed_each_product.xpath(xpath_product_info_ProdType_value)[0]

        # Price Excluding Tax Text & Value
        xpath_product_info_PriceExtax = '//table[@class="table table-striped"]//tr[3]/th/text()'
        product_info_PriceExtax = parsed_each_product.xpath(xpath_product_info_PriceExtax)[0]
        xpath_product_info_PriceExtax_value = '//table[@class="table table-striped"]//tr[3]/td/text()'
        product_info_PriceExtax_value = parsed_each_product.xpath(xpath_product_info_PriceExtax_value)[0]

        # Price Including Tax Text & Value
        xpath_product_info_PriceIntax = '//table[@class="table table-striped"]//tr[4]/th/text()'
        product_info_PriceIntax = parsed_each_product.xpath(xpath_product_info_PriceIntax)[0]
        xpath_product_info_PriceIntax_value = '//table[@class="table table-striped"]//tr[4]/td/text()'
        product_info_PriceIntax_value = parsed_each_product.xpath(xpath_product_info_PriceIntax_value)[0]

        # Tax Text & Value
        xpath_product_info_Tax = '//table[@class="table table-striped"]//tr[5]/th/text()'
        product_info_Tax = parsed_each_product.xpath(xpath_product_info_Tax)[0]
        xpath_product_info_Tax_value = '//table[@class="table table-striped"]//tr[5]/td/text()'
        product_info_Tax_value = parsed_each_product.xpath(xpath_product_info_Tax_value)[0]

        # Availability Text & Value
        xpath_product_info_Availability = '//table[@class="table table-striped"]//tr[6]/th/text()'
        product_info_Availability = parsed_each_product.xpath(xpath_product_info_Availability)[0]
        xpath_product_info_Availability_value = '//table[@class="table table-striped"]//tr[6]/td/text()'
        product_info_Availability_value = parsed_each_product.xpath(xpath_product_info_Availability_value)[0]

        # Number of Reviews Text & Value
        xpath_product_info_NumReviews = '//table[@class="table table-striped"]//tr[7]/th/text()'
        product_info_NumReviews = parsed_each_product.xpath(xpath_product_info_NumReviews)[0]
        xpath_product_info_NumReviews_value = '//table[@class="table table-striped"]//tr[7]/td/text()'
        product_info_NumReviews_value = parsed_each_product.xpath(xpath_product_info_NumReviews_value)[0]

        this_product_details = {
            "Product_Name": product_name,
            "Product_Category": category_name,
            "Product_Link": this_product,
            "Product_Price": product_price,
            "Currency_Symbol": currency_symbol,
            "in_Stock": product_in_stock,
            "Available_Quantity": product_quantity,
            "Avg_Rating": product_ratings,
            "Image_URL": product_image_link,
            "Product_Description": product_description,
            # "Product Information": [
            #     {product_info_UPC: product_info_UPC_value},
            #     {product_info_ProdType: product_info_ProdType_value},
            #     {product_info_PriceExtax: product_info_PriceExtax_value},
            #     {product_info_PriceIntax: product_info_PriceIntax_value},
            #     {product_info_Tax: product_info_Tax_value},
            #     {product_info_Availability: product_info_Availability_value},
            #     {product_info_NumReviews: product_info_NumReviews_value}
            # ],
            "Category_Link": each_category_link
        }
        this_category_products.append(this_product_details)
        output.append(this_product_details)


def ensure_dir_exists(path: str):
    # Check if directory exists, if not, create it
    if not os.path.exists(path):
        os.makedirs(path)
        print(f'Directory {path} Created')  # Print confirmation of directory creation


def page_checker(url: str, method: str, directory_path: str):
    # Create a unique hash for the URL to use as the filename
    page_hash = hashlib.sha256(string=url.encode(encoding='UTF-8', errors='backslashreplace')).hexdigest()
    ensure_dir_exists(path=directory_path)  # Ensure the directory exists
    file_path = os.path.join(directory_path, f"{page_hash}.html.gz")  # Define file path
    if os.path.exists(file_path):  # Check if the file already exists
        print("File exists, reading it...")  # Notify that the file is being read
        print(f"Filename is {page_hash}")
        with gzip.open(filename=file_path, mode='rb') as file:
            file_text = file.read().decode(encoding='UTF-8', errors='backslashreplace')  # Read and decode file
        return file_text  # Return the content of the file
    else:
        print("File does not exist, Sending request & creating it...")  # Notify that a request will be sent
        _response = req_sender(url=url, method=method)  # Send the HTTP request
        if _response is not None:
            print(f"Filename is {page_hash}")
            with gzip.open(filename=file_path, mode='wb') as file:
                if isinstance(_response, str):
                    file.write(_response.encode())  # Write response if it is a string
                    return _response
                file.write(_response.content)  # Write response content if it is bytes
            return _response.text  # Return the response text


def scrape_func(url: str, method: str, path: str):
    # Get HTTP response text
    html_response_text = page_checker(url=url, method=method, directory_path=path)
    # Parse HTML content using lxml
    parsed_html = html.fromstring(html=html_response_text)
    xpath_categories = '//ul[@class="nav nav-list"]//ul//@href'
    categories_list = parsed_html.xpath(xpath_categories)
    categories_links = ['https://books.toscrape.com/' + cat_link for cat_link in categories_list]
    # print(categories_links)
    category_path = os.path.join(path, "category_data")
    pages_path = os.path.join(path, "pages_path")

    final_output = []
    for each_category_link in categories_links:
        each_category_response = page_checker(url=each_category_link, method=method, directory_path=category_path)
        parsed_cat_html = html.fromstring(html=each_category_response)

        xpath_this_category_name = '//div[@class="page-header action"]/h1/text()'
        this_category_name = parsed_cat_html.xpath(xpath_this_category_name)[0]
        print(this_category_name)

        xpath_pages = '//li[@class="current"]/text()'
        no_of_pages = int((parsed_cat_html.xpath(xpath_pages)[0].strip())[-2:]) if parsed_cat_html.xpath(xpath_pages) else []
        print(no_of_pages)
        this_category_details = {}
        this_category_products = []
        if no_of_pages:
            for page_no in range(1, no_of_pages + 1):
                next_page_link = each_category_link.replace('index.html', f'page-{page_no}.html')

                this_page_response = page_checker(url=next_page_link, method=method, directory_path=category_path)
                parsed_this_page = html.fromstring(html=this_page_response)

                xpath_products_link = '//article[@class="product_pod"]/h3/a/@href'
                products_link_list = parsed_this_page.xpath(xpath_products_link)
                this_page_product_links = ['https://books.toscrape.com/catalogue/' + products_link.replace('../../../', '') for products_link in products_link_list]
                product_data_scrape(this_page_product_links=this_page_product_links, this_category_products=this_category_products, method=method, directory_path=pages_path, each_category_link=each_category_link, category_name=this_category_name, output=final_output)
        else:
            xpath_products_link = '//article[@class="product_pod"]/h3/a/@href'
            products_link_list = parsed_cat_html.xpath(xpath_products_link)
            this_page_product_links = ['https://books.toscrape.com/catalogue/' + products_link.replace('../../../', '') for products_link in products_link_list]
            product_data_scrape(this_page_product_links=this_page_product_links, this_category_products=this_category_products, method=method, directory_path=pages_path, each_category_link=each_category_link, category_name=this_category_name, output=final_output)
        this_category_details[this_category_name] = this_category_products
        # final_output.append(this_category_details)

    # Storing in JSON file
    with open('final_output_new.json', 'w+') as file:
        file.write(json.dumps(final_output))

    # Storing in MySql Database using SQL
    # Connecting to the Database
    my_host = 'localhost'
    my_user = 'root'
    my_password = 'actowiz'
    my_database = 'booktoscrape'
    my_charset = 'utf8mb4'

    connection = pymysql.connect(host=my_host, user=my_user, database=my_database, password=my_password, charset=my_charset, cursorclass=pymysql.cursors.DictCursor)
    cursor = connection.cursor()

    print('storing into Database')
    for data in final_output:
        print(data)
        cols = data.keys()
        rows = data.values()
        insert_query = f'''INSERT INTO `bookscrape` ({', '.join(tuple(cols))}) VALUES ({('%s, ' * len(data)).rstrip(", ")});'''
        print(insert_query)
        cursor.execute(query=insert_query, args=tuple(rows))
        connection.commit()


# Define main URL, method, and path
my_url = "https://books.toscrape.com/index.html"
my_method = "GET"
my_path = os.getcwd()
# Call the scraping function with specified parameters
scrape_func(url=my_url, method=my_method, path=my_path)
