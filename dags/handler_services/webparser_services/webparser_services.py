from selenium.webdriver.chrome.options import Options
from selenium import webdriver
from selenium.webdriver.common.by import By
from handler_services.data_file_info import StatusFile,DataBykeUrlsClass
from handler_services.utils_byke_data import get_current_time

def complete_data_info(url,file_name) -> DataBykeUrlsClass:
    year = str(int(file_name[:4])) if file_name[:4].isdigit() else None
    month = str(int(file_name[:6])) if file_name[:6].isdigit() else None
    time_now = get_current_time()
    return DataBykeUrlsClass(file_name=file_name,url=url,month=month,year=year,
                             status=StatusFile.CREATED,created_at=time_now,updated_at=time_now)

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
        links_2 = [complete_data_info(link_file[0],link_file[1]) for link_file in links]
        print(links_2)
        return links_2