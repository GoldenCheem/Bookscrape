import scrapy
from book.items import BookItem

class SpiderSpider(scrapy.Spider):
    name = "bookspider"
    allowed_domains = ["books.toscrape.com"]
    start_urls = ["https://books.toscrape.com/"]

    def parse(self, response):
        books = response.css('article.product_pod')

        for book in books:
            relative_url = book.css('h3 a').attrib['href']

            if 'catalogue/' in relative_url:
                book_url = 'https://books.toscrape.com/' + relative_url
            else:
                book_url = 'https://books.toscrape.com/catalogue/' + relative_url
            
            yield response.follow(book_url, callback=self.parse_bookpage)

        next_page = response.css('li.next a').attrib['href']
        next_page_url = ''

        if next_page is not None:
            if 'catalogue/' in next_page:
                next_page_url = 'https://books.toscrape.com/' + next_page
            else:
                next_page_url = 'https://books.toscrape.com/catalogue/' + next_page
            
            yield response.follow(next_page_url, callback=self.parse)


    def parse_bookpage(self, response):
        table_rows = response.css('table tr')

        book_item = BookItem()
        book_item['title'] = response.css('h1::text').get()
        book_item['url'] = response.url
        book_item['category'] = response.xpath('//*[@id="default"]/div/div/ul/li[3]/a/text()').get()
        book_item['product_type'] = table_rows[1].css('td::text').get()
        book_item['price_excl_tax'] = table_rows[2].css('td::text').get()
        book_item['price_incl_tax'] = table_rows[3].css('td::text').get()
        book_item['tax'] = table_rows[4].css('td::text').get()
        book_item['availability'] = table_rows[5].css('td::text').get()
        book_item['num_reviews'] = table_rows[6].css('td::text').get()
        book_item['star_rating'] = response.css('p.star-rating').attrib['class']

        yield book_item
