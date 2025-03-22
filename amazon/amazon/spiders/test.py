import scrapy
from scrapy.crawler import CrawlerProcess
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import json
from datetime import datetime, timedelta
import os
import re
import random

# Specify ASINs to scrape directly
specific_asins = []

INPUT_FILE = "data/amz_asin.jsonl"  # JSONLines format
MAX_PRODUCTS = 2000
OUTPUT_DIR = "data"
data_collected = []  # Store scraped data globally

# Rotating User Agents**
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36",
]
class AmazonProductsSpider(scrapy.Spider):
    name = "amz_products"

    # Custom settings for the spider
    custom_settings = {
        'LOG_LEVEL': 'INFO',
        'CONCURRENT_REQUESTS': 1,
        'DOWNLOAD_DELAY': random.uniform(1, 2),  # Random delay to avoid detection,
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.product_count = 0
        self.specific_asins = specific_asins
        
        if self.specific_asins:
            self.logger.info("Using specific ASINs provided. Skipping all prompts.")
            self.asins_to_scrape = self.specific_asins  # ✅ Highest Priority
        else:
            self.logger.info("No specific ASINs provided. Proceeding with prompt-based loading.")
            self.asins_to_scrape = self._load_asins_with_serial_filter_or_prompt()  # ✅ Second Priority

        self.output_file = self._generate_output_filename()
        
        # ✅ Ensure start_serial_number is always initialized
        self.start_serial_number = self._get_last_serial_number(log_message=False)  # ✅ Prevent duplicate log and ✅ No logging during initialization

        # Initialize Selenium WebDriver
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument(f"user-agent={random.choice(USER_AGENTS)}")
        self.driver = webdriver.Chrome(options=chrome_options)

    def _load_asins_with_serial_filter_or_prompt(self):
        """Load ASINs from the input file, allow filtering by serial number range, 
        and resume from last stop if chosen."""
        
        try:
            asins = []
            serial_numbers = []

            with open(INPUT_FILE, "r", encoding="utf-8") as file:
                for line in file:
                    record = json.loads(line)
                    serial_numbers.append(record.get("serial_number", ""))
                    if record.get("re_scrape", True):
                        asins.append(record["asin"])

            # ✅ Ask if the user wants to filter by serial number range
            use_range = input("Do you want to filter by serial number range? (yes/no): ").strip().lower()

            if use_range == "yes":
                start_sn = input("Enter start serial number (e.g., ASN50): ").strip()
                end_sn = input("Enter end serial number (e.g., ASN100): ").strip()

                if start_sn in serial_numbers and end_sn in serial_numbers:
                    start_idx = serial_numbers.index(start_sn)
                    end_idx = serial_numbers.index(end_sn)
                    asins = asins[start_idx:end_idx + 1]
                    self.logger.info(f"🌚 Filtered ASINs from serial {start_sn} to {end_sn}")
                else:
                    self.logger.warning("⚠️ Invalid serial number range provided. Using all ASINs.")

            else:
                # ✅ Ask if the user wants to continue from the last stop
                choice = input("Do you want to continue from the last stop? (yes/no): ").strip().lower()
                if choice == "yes":
                    last_serial_number = self._get_last_serial_number(log_message=True)  # Get last scraped serial (e.g., 1294)

                    if last_serial_number > 0:  # ✅ Ensure valid last serial
                        numeric_serial = last_serial_number  # ✅ Use it directly (already an integer)

                        # ✅ Find the corresponding ASIN position in `amz_asin.jsonl`
                        last_serial_index = serial_numbers.index(f"ASN{numeric_serial}") if f"ASN{numeric_serial}" in serial_numbers else -1
                        if last_serial_index != -1:
                            asins = asins[last_serial_index + 1:]  # ✅ Resume from the next ASIN
                            self.logger.info(f"🚣 Resuming from serial number ASN{numeric_serial + 1}")
                        else:
                            self.logger.warning(f"☢️ Could not find ASN{last_serial_number} in the input file. Starting from the beginning.")

            if not asins:
                self.logger.error("⚠️ No ASINs found to scrape. Exiting...")
                exit(1)  # ✅ Exit gracefully if no ASINs are found

            return asins

        except (FileNotFoundError, json.JSONDecodeError) as e:
            self.logger.error(f"⚠️ Error loading ASINs: {e}")
            exit(1)  # ✅ Exit if an error occurs

    def _generate_output_filename(self):
        """Generate a unique output filename based on the current GMT+3 date and time."""
        now = datetime.utcnow() + timedelta(hours=3)
        timestamp = now.strftime("%d-%m-%Y_%I-%M-%p")
        return os.path.join(OUTPUT_DIR, f"amz_products_{timestamp}.json")

    def _get_last_serial_number(self, log_message=True):
        """Find the last serial number from the most recent output file and return an integer."""
        try:
            files = [f for f in os.listdir(OUTPUT_DIR) if f.startswith("amz_products_") and f.endswith(".json")]
            if not files:
                return 0  # ✅ Default to integer 0 if no previous data exists

            files.sort(key=lambda f: os.path.getctime(os.path.join(OUTPUT_DIR, f)), reverse=True)
            latest_file = os.path.join(OUTPUT_DIR, files[0])

            with open(latest_file, "r", encoding="utf-8") as file:
                existing_data = json.load(file)
                if isinstance(existing_data, list) and len(existing_data) > 0:
                    last_serial = max(
                        int(re.search(r"\d+", item["serial_number"]).group()) for item in existing_data
                    )
                    if log_message:  # ✅ Log only when needed
                        self.logger.info(f"🌗 Last serial number found: AP{last_serial}")
                    return last_serial  # ✅ Return as an integer (not a string with "AP")
        except Exception as e:
            self.logger.error(f"⚠️ Error reading last serial number: {e}")
        return 0  # ✅ Ensure integer fallback

    def start_requests(self):
        """Start scraping process. No duplicate prompt for continuing from the last stop."""
        base_url = "https://www.amazon.sa/dp/"

        # If no ASINs to scrape, log an error and exit early
        if not self.asins_to_scrape:
            self.logger.error("⚠️ No ASINs available to scrape. Exiting...")
            return

        # Start Scraping
        for asin in self.asins_to_scrape:
            if self.product_count >= MAX_PRODUCTS:
                break
            url = f"{base_url}{asin}?language=en_AE&th=1&psc=1"
            headers = {"User-Agent": random.choice(USER_AGENTS)}
            yield scrapy.Request(url, callback=self.parse_product_details, meta={"asin": asin}, headers=headers)

    def parse_product_details(self, response):
        global data_collected
        product_details = {}

        if self.product_count >= MAX_PRODUCTS:
            return

        self.product_count += 1
        asin = response.meta["asin"]

        self.logger.info(f"😘 Processing ASIN: {asin}")
        
        try:
            # Increment serial number
            self.start_serial_number = int(self.start_serial_number)  # ✅ Convert to integer to ensure self.start_serial_number is an integer before using it
            self.start_serial_number += 1
            serial_number = f"AP{self.start_serial_number}"  # ✅ Safe string concatenation
            self.logger.info(f"💯 Serial Number: {serial_number}")

            # Scraping date
            scraping_date = datetime.utcnow().strftime("%d/%m/%Y")
            self.logger.info(f"📅 Current Scraping Date: {scraping_date}")

            # Convert UTC to Saudi Arabia Time (AST, UTC+3)
            saudi_time = datetime.utcnow() + timedelta(hours=3)
            scraping_time = saudi_time.strftime("%I:%M %p")
            self.logger.info(f"🕰️ Current Scraping Time: {scraping_time}")

            # Extracting the title of the product
            product_title = response.css("span#productTitle::text").get(default="").strip()
            self.logger.info(f"📕 Extracting Product Title: {product_title}")

            # Availability extraction
            availability_selectors = [
                "div#availability span::text",  # Main availability section
                "div#availability div.a-section span::text",  # Alternative availability text
                "#availabilityInsideBuyBox_feature_div span::text",  # Availability inside buy box
                "#desktop_buybox div.a-section span::text",  # Another alternative location
            ]

            availability_text = None
            for selector in availability_selectors:
                extracted_text = response.css(selector).get()
                if extracted_text:
                    availability_text = extracted_text.strip()
                    break

            if availability_text:
                availability_number = re.search(r"\d+", availability_text)
                availability = availability_number.group() if availability_number else "Available"
            else:
                availability = "Currently unavailable"

            # If product is unavailable, set price to "N/A"
            if "currently unavailable" in availability.lower():
                price = "N/A"
            else:
                # Extract current price only if product is available
                price_selectors = [
                    "#corePrice_feature_div .a-spacing-micro span span::text",
                    ".apexPriceToPay span::text",
                    ".priceToPay span span::text",
                    "#corePrice_feature_div span span::text",
                    "span.a-price span.a-offscreen::text",
                    "span#price_inside_buybox::text",
                    "span#newBuyBoxPrice::text",
                    "span.priceToPay span.a-offscreen::text",
                ]

                price = None
                for selector in price_selectors:
                    raw_price = response.css(selector).get()
                    if raw_price:
                        price = re.sub(r"SAR", "", raw_price).strip()
                        break

                # Ensure price has a fallback value
                price = price if price else "N/A"

            # Log the final extracted price
            self.logger.info(f"💸 Extracted price: {price}")

            # Extract previous price
            previous_price_selectors = [
                ".basisPrice span span::text",
                "span.a-price.a-text-price span.a-offscreen::text",
                "span#priceblock_dealprice::text",
            ]
            raw_previous_price = None

            for selector in previous_price_selectors:
                raw_previous_price = response.css(selector).get()
                if raw_previous_price:
                    break

            previous_price = re.sub(r"SAR", "", raw_previous_price).strip() if raw_previous_price else "N/A"
            self.logger.info(f"🏷️ Extracted previous price: {previous_price}")

            # Extracting Discount
            raw_discount = response.css("span.savingsPercentage::text").get(default="0%").strip()
            discount = re.sub(r"[\u200e]", "", raw_discount).strip()
            self.logger.info(f"🔖 Extracted discount: {discount}")

            # Handle cases where price and previous price are identical or discount is 0%
            if previous_price == price or discount == "0%":
                # self.logger.warning(f"Previous price is incorrect (either matches current price or discount is 0%). Setting previous price to 'N/A'.")
                previous_price = "N/A"

            # Extracting Currency
            currency = response.css("#corePrice_feature_div .a-price-symbol::text").get(default="N/A").strip()
            self.logger.info(f"💲 Extracted Currency: {currency}")

            # Extracting Product link
            product_link = response.url
            self.logger.info(f"🔗 Extracted Product link: {product_link}")

            # Extracting Category
            category_list = response.css("div#wayfinding-breadcrumbs_feature_div ul.a-unordered-list li a::text").getall()
            category_list = [cat.strip() for cat in category_list if cat.strip()]  # Clean whitespace and filter empty items

            # Ensure we always have 5 category slots (fill missing ones with "N/A")
            category_01, category_02, category_03, category_04, category_05, category_06 = (category_list + ["N/A"] * 6)[:6]
            self.logger.info(f"📊 Extracted Categories: {category_list}")

            # Sold by, Sold by link and seller ID extraction
            sold_by = "Amazon.sa" if "Amazon.sa" in response.css("#merchantInfoFeature_feature_div .offer-display-feature-text-message::text").get(default="N/A") else response.css("a#sellerProfileTriggerId::text").get(default="N/A")
            sold_by_link = response.css("a#sellerProfileTriggerId::attr(href)").get()
            if sold_by_link:
                sold_by_link = "https://www.amazon.sa" + sold_by_link
                seller_id_match = re.search(r"seller=([A-Z0-9]+)", sold_by_link)
                seller_id = seller_id_match.group(1) if seller_id_match else ""
            else:
                sold_by_link = "N/A"
                seller_id = "N/A"

            self.logger.info(f"🛒 Extracted sold_by: {sold_by}")
            self.logger.info(f"🔗 Extracted sold_by_link: {sold_by_link}")
            self.logger.info(f"🆔 Extracted seller_id: {seller_id}")

            # Extract and clean brand name
            try:
                raw_brand = response.css("a#bylineInfo::text").get(default="").strip()
                if raw_brand:
                    # Remove unwanted texts like "Visit the", "Store", and "Brand:"
                    brand = re.sub(r"(Visit the|Store|Brand:)", "", raw_brand).strip()
                else:
                    brand = "N/A"
                self.logger.info(f"💝 Extracted brand: {brand}")

            except Exception as e:
                self.logger.error(f"⚠️ Error extracting brand: {e}")

            # Extract bought_in_past_month and clean the text
            try:
                raw_bought = response.css("#social-proofing-faceout-title-tk_bought .a-text-bold::text").get(default="0")
                match = re.search(r"(\d+\+)", raw_bought)
                bought_in_past_month = match.group(1) if match else "0"
            except Exception as e:
                bought_in_past_month = "0"
                self.logger.error(f"⚠️ Error extracting bought_in_past_month: {e}")
            self.logger.info(f"🏛️ Extracted bought in past month: {bought_in_past_month}")

            # Extract images link
            image_link = response.css("img#landingImage::attr(src)").get()
            self.logger.info(f"🖼️ Extracted Image Link: {image_link}")

            # Extract Total Rating
            raw_total_rating = response.css("span#acrCustomerReviewText::text").get(default="0")
            total_rating = re.sub(r"[^\d]", "", raw_total_rating).strip()  # Remove non-numeric characters (like commas)
            total_rating = total_rating if total_rating else "0"  # Ensure it's not empty
            
            # Extract Star Rating
            star_rating = response.css("span.a-icon-alt::text").re_first(r"([0-5]\.\d?)")
            # If total_rating is "0", set star_rating to "0"
            if total_rating == "0":
                star_rating = "0"
            else:
                star_rating = star_rating.strip() if star_rating else "0"

            self.logger.info(f"⭐ Extracted Star Rating: {star_rating}")           
            self.logger.info(f"💹 Extracted Total Rating: {total_rating}")

            # Log the extracted Availability
            self.logger.info(f"ℹ️ Extracted availability: {availability}")
            
            # Refresh Selenium Driver for each ASIN
            self.driver.get(response.url)

            try:
                # Wait for the Best Sellers Rank section to load
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#productDetails_detailBullets_sections1, #detailBulletsWrapper_feature_div"))
                )
                self.logger.info("😯 Best Sellers Rank section loaded.")

                # Selenium for Best Sellers Rank extraction
                best_sellers_rank_01_no = "N/A"
                best_sellers_rank_01_category = "N/A"
                best_sellers_rank_02_no = "N/A"
                best_sellers_rank_02_category = "N/A"

                try:
                    # First structure
                    try:
                        rank_01_element = self.driver.find_element(By.CSS_SELECTOR, "#detailBullets_feature_div+ .detail-bullet-list .a-list-item")
                        if rank_01_element:
                            rank_text_01 = rank_01_element.text
                            match_01_no = re.search(r"#([\d,]+)", rank_text_01)
                            match_01_category = re.search(r"in (.+?) \(", rank_text_01)
                            best_sellers_rank_01_no = match_01_no.group(1).replace(",", "").strip() if match_01_no else "N/A"
                            best_sellers_rank_01_category = match_01_category.group(1).strip() if match_01_category else "N/A"

                        rank_02_element = self.driver.find_element(By.CSS_SELECTOR, "#detailBullets_feature_div+ .detail-bullet-list ul li .a-list-item")
                        if rank_02_element:
                            rank_text_02 = rank_02_element.text
                            match_02_no = re.search(r"#([\d,]+)", rank_text_02)
                            match_02_category = re.search(r"in (.+?)$", rank_text_02)
                            best_sellers_rank_02_no = match_02_no.group(1).replace(",", "").strip() if match_02_no else "N/A"
                            best_sellers_rank_02_category = match_02_category.group(1).strip() if match_02_category else "N/A"
                    except Exception as e:
                        self.logger.info("😞 First structure failed. Trying second structure.")

                    # Second structure
                    try:
                        rank_01_element_alt = self.driver.find_element(By.CSS_SELECTOR, "#productDetails_detailBullets_sections1 tr:nth-child(2) td span span")
                        if rank_01_element_alt:
                            rank_text_01_alt = rank_01_element_alt.text
                            match_01_alt_no = re.search(r"#([\d,]+)", rank_text_01_alt)
                            match_01_alt_category = re.search(r"in (.+?) \(", rank_text_01_alt)
                            best_sellers_rank_01_no = match_01_alt_no.group(1).replace(",", "").strip() if match_01_alt_no else "N/A"
                            best_sellers_rank_01_category = match_01_alt_category.group(1).strip() if match_01_alt_category else "N/A"

                        rank_02_element_alt = self.driver.find_element(By.CSS_SELECTOR, "#productDetails_detailBullets_sections1 tr:nth-child(2) td br + span")
                        if rank_02_element_alt:
                            rank_text_02_alt = rank_02_element_alt.text
                            match_02_alt_no = re.search(r"#([\d,]+)", rank_text_02_alt)
                            match_02_alt_category = re.search(r"in (.+?)$", rank_text_02_alt)
                            best_sellers_rank_02_no = match_02_alt_no.group(1).replace(",", "").strip() if match_02_alt_no else "N/A"
                            best_sellers_rank_02_category = match_02_alt_category.group(1).strip() if match_02_alt_category else "N/A"
                    except Exception as e:
                        self.logger.info("😩 Second structure failed. Trying third structure.")

                    # Third structure
                    try:
                        # Find the table containing Best Sellers Rank
                        rank_table = self.driver.find_element(By.ID, "productDetails_detailBullets_sections1")

                        # Locate the specific row for Best Sellers Rank
                        rank_row = rank_table.find_element(By.XPATH, ".//tr[th[contains(text(),'Best Sellers Rank')]]")

                        # Extract the text content for the first rank
                        rank_spans = rank_row.find_elements(By.XPATH, ".//span/span")
                        if rank_spans and len(rank_spans) > 0:
                            rank_text_01_third = rank_spans[0].text  # Get the first span text
                            match_01_third_no = re.search(r"#([\d,]+)", rank_text_01_third)
                            match_01_third_category = re.search(r"in (.+?) \(", rank_text_01_third)
                            best_sellers_rank_01_no = match_01_third_no.group(1).replace(",", "").strip() if match_01_third_no else "N/A"
                            best_sellers_rank_01_category = match_01_third_category.group(1).strip() if match_01_third_category else "N/A"

                        # Extract the text content for the second rank
                        if rank_spans and len(rank_spans) > 1:
                            rank_text_02_third = rank_spans[1].text  # Get the second span text
                            match_02_third_no = re.search(r"#([\d,]+)", rank_text_02_third)
                            match_02_third_category = re.search(r"in (.+?)$", rank_text_02_third)
                            best_sellers_rank_02_no = match_02_third_no.group(1).replace(",", "").strip() if match_02_third_no else "N/A"
                            best_sellers_rank_02_category = match_02_third_category.group(1).strip() if match_02_third_category else "N/A"
                        else:
                            best_sellers_rank_02_no = "N/A"
                            best_sellers_rank_02_category = "N/A"

                    except Exception as e:
                        self.logger.info("🤬 Third structure failed.")

                    self.logger.info(f"👍 Best Sellers Rank 01: {best_sellers_rank_01_no} in {best_sellers_rank_01_category}")
                    self.logger.info(f"👍 Best Sellers Rank 02: {best_sellers_rank_02_no} in {best_sellers_rank_02_category}")

                except Exception as e:
                    self.logger.error(f"⚠️ Unexpected error extracting best seller rank: {e}")
            except Exception as e:
                self.logger.error(f"⚠️ Timeout waiting for Best Sellers Rank section: {e}")

            # Extract and format `date_first_available`
            try:
                raw_date = None
                formatted_date = "N/A"

                # List of CSS selectors to try
                selectors = [
                    "#productDetails_detailBullets_sections1 .prodDetAttrValue::text",
                    "#productDetails_detailBullets_sections1 tr+ tr .prodDetAttrValue::text",
                    "#detailBullets_feature_div li:nth-child(1) .a-text-bold+ span::text",
                    "#detailBullets_feature_div li:nth-child(2) .a-text-bold+ span::text",
                    "#detailBullets_feature_div li:nth-child(3) .a-text-bold+ span::text",
                    "#detailBullets_feature_div li:nth-child(4) .a-text-bold+ span::text",
                    "#detailBullets_feature_div li:nth-child(5) .a-text-bold+ span::text",
                    "li:nth-child(1) .a-text-bold+ span::text",
                    "li:nth-child(2) .a-text-bold+ span::text",
                    "li:nth-child(3) .a-text-bold+ span::text",
                    "li:nth-child(4) .a-text-bold+ span::text",
                    "li:nth-child(5) .a-text-bold+ span::text",
                    "li:nth-child(6) .a-text-bold+ span::text",
                    "#detailBulletsWrapper_table tr:contains('Date First Available') td::text",
                    "#productDetails_detailBullets_sections1 tr:contains('Date First Available') td::text",
                ]

                # Loop through selectors and validate the extracted value
                for selector in selectors:
                    raw_date = response.css(selector).get()
                    if raw_date:
                        raw_date = raw_date.strip()
                        self.logger.info(f"Attempting to parse 'date_first_available': {raw_date}")

                        # Check if the raw_date matches common date patterns
                        if re.match(r"^\d{1,2} [A-Za-z]+ \d{4}$", raw_date):  # Example: "10 August 2023"
                            formatted_date = datetime.strptime(raw_date, "%d %B %Y").strftime("%d/%m/%Y")
                            break
                        else:
                            self.logger.info(f"😶 Extracted value is not a date: {raw_date}")
                    else:
                        self.logger.info(f"🤯 No value found with selector: {selector}")

                # If no valid date was found, log a warning
                if formatted_date == "N/A":
                    self.logger.warning("Date First Available not found or could not be parsed.")

            except Exception as e:
                self.logger.error(f"⚠️ Error extracting `date_first_available`: {e}")
                formatted_date = "N/A"

            # Extract #1 Best Seller
            first_best_seller = "YES" if "Best Seller" in response.css(".p13n-best-seller-badge::text").get(default="") else "NO"
            self.logger.info(f"🙊 Extracted #1 Best Seller: {first_best_seller}")

            # Extract Amazon Choice
            amazon_choice = "YES" if "Choice" in response.css("div#acBadge_feature_div span::text").get(default="") else "NO"
            self.logger.info(f"🐵 Extracted Amazon Choice: {amazon_choice}")
            
            # Extract Limited Time Deal
            limited_time_deal = "YES" if response.css("#dealBadgeSupportingText span::text").get() else "NO"
            self.logger.info(f"🙉 Extracted Limited Time Deal: {limited_time_deal}")


            # Construct Product Details
            product_details = {
                "serial_number": serial_number,
                "scraping_date": scraping_date,
                "scraping_time": scraping_time,
                "title": product_title,
                "price": price,
                "previous_price": previous_price,
                "discount": discount,
                "currency": currency,
                "product_link": product_link,
                "category_01": str(category_01),
                "category_02": str(category_02),
                "category_03": str(category_03),
                "category_04": str(category_04),
                "category_05": str(category_05),
                "category_06": str(category_06),
                "sold_by": sold_by,
                "sold_by_link": sold_by_link,
                "seller_id": seller_id,
                "brand": brand,
                "bought_in_past_month": bought_in_past_month,
                "image_link": image_link,
                "star_rating": star_rating,
                "total_rating": total_rating,
                "availability": availability,
                "best_sellers_rank_01_no": best_sellers_rank_01_no,
                "best_sellers_rank_01_category": best_sellers_rank_01_category,
                "best_sellers_rank_02_no": best_sellers_rank_02_no,
                "best_sellers_rank_02_category": best_sellers_rank_02_category,
                "date_first_available": formatted_date,
                "#1_best_seller": str(first_best_seller),
                "amazons_choice": str(amazon_choice),
                "limited_time_deal": str(limited_time_deal),
                "ASIN": asin,
            }

            # Log product details for debugging
            self.logger.debug(f"Product details for ASIN {asin}: {json.dumps(product_details, indent=4)}")

            # Save Data
            if product_details not in data_collected:
                data_collected.append(product_details)
                with open(self.output_file, "w", encoding="utf-8") as file:
                    json.dump(data_collected, file, indent=4, ensure_ascii=False)
            self.logger.info(f"📌 Product details saved to {self.output_file}")
        except Exception as e:
            self.logger.error(f"⚠️ Error parsing product details for ASIN {response.meta['asin']}: {e}")

    def closed(self, reason):
        self.driver.quit()

if __name__ == "__main__":
    process = CrawlerProcess(settings={"LOG_LEVEL": "INFO"})
    process.crawl(AmazonProductsSpider)
    process.start()

    print(f"🏁 Scraping completed. Data saved to '{AmazonProductsSpider().output_file}'")