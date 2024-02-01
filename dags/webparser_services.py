from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.common.by import By


class WebParser:
    def __init__(self,headless=False):
        self.headless = headless
        self.driver = None

    def __enter__(self):
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless=new")
        server ='http://172.24.192.1:4444'
        self.driver = webdriver.Remote(command_executor=server, options=chrome_options)
        # self.driver = webdriver.Firefox()
        self.driver.implicitly_wait(1)
        return self
    def __exit__(self, *_):
        if self.driver:
            self.driver.quit()


class PageBykeData:
    def __init__(self,driver):
        self.driver:webdriver = driver
        self.elementsFiles = driver.find_elements(By.CSS_SELECTOR,"tbody>tr>td>a")
    def get_all_links_files_data(self):
        get_link = lambda element : (element.get_attribute("href"),element.text)
        links = list(map(get_link, self.elementsFiles)) # getting all links for file byke
        del links[len(links)-1]
        return links