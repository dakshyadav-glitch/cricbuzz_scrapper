# WebDriver management

from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from config import PAGE_LOAD_TIMEOUT, WEBDRIVER_WAIT


class DriverManager:
    
    def __init__(self):
        self.driver = None
        self.wait = None
    
    def setup(self, headless=False):
        options = webdriver.ChromeOptions()
        options.add_argument("--start-maximized")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        if headless:
            options.add_argument("--headless=new")
        options.page_load_strategy = "eager"
        
        self.driver = webdriver.Chrome(options=options)
        self.driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)
        self.wait = WebDriverWait(self.driver, WEBDRIVER_WAIT)
        
        return self.driver, self.wait
    
    def quit(self):
        if self.driver:
            self.driver.quit()
            self.driver = None
            self.wait = None
    
    def get(self, url):
        if self.driver:
            self.driver.get(url)
    
    def get_page_source(self):
        if self.driver:
            return self.driver.page_source
        return ""
    
    def get_page_text(self):
        if self.driver:
            from selenium.webdriver.common.by import By
            return self.driver.find_element(By.TAG_NAME, "body").text
        return ""
    
    def find_element(self, by, value):
        if self.driver:
            return self.driver.find_element(by, value)
        return None
    
    def find_elements(self, by, value):
        if self.driver:
            return self.driver.find_elements(by, value)
        return []


driver_manager = DriverManager()
