from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.common.by import By
from handler_services.data_byke_services.data_file_info import DataBykeUrlsClass

class WebDriverSetup:
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

class BykeDataPage:
    def __init__(self,driver):
        self.driver:webdriver = driver
        self.elementsFiles = driver.find_elements(By.CSS_SELECTOR,"tbody>tr>td>a")
    def get_all_links_files_data(self) -> list[DataBykeUrlsClass]:
        get_link = lambda element : (element.get_attribute("href"),element.text)
        links = list(map(get_link, self.elementsFiles)) # getting all links for file byke
        links = [link_file for link_file in links if not link_file[1].startswith("JC")]
        del links[len(links)-1]
        links_2 = [DataBykeUrlsClass.from_tuple_link_file_name(link_file[0],link_file[1]) for link_file in links]
        print(links_2)
        return links_2