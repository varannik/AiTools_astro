# scraper.py
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup




# Set up Chrome options
chrome_options = Options()
# Run Chrome in headless mode, without a UI or display server dependencies
chrome_options.add_argument("--headless")
# Disable the Chrome sandbox, useful in a containerized environment
chrome_options.add_argument("--no-sandbox")
# Overcome limited resource problems
chrome_options.add_argument("--disable-dev-shm-usage")
# Set up WebDriver with the specified Chrome options
# driver = webdriver.Chrome(options=chrome_options)

options = webdriver.ChromeOptions()
# driver = webdriver.Remote("http://localhost:4444", options=options)

driver = webdriver.Remote(command_executor='http://172.19.0.6:4444/wd/hub', options=options)






from datetime import timedelta
import pandas as pd
import os
# import urllib3.request


headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0",
           "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
           "Accept-Language": "en-US,en;q=0.9"
           }




# Define the path to the Excel file
file_path = 'data.xlsx'  # Replace with the actual file path

# Check if the file exists
if os.path.isfile(file_path):

    print(f"The Excel file '{file_path}' exists.")
    # Define the data types for each column
    data_types = {'Task':str, 'Url':str, 'ToolsName':str, 'Url_name':str}

    # Read the Excel file without index
    table_dfs = pd.read_excel('data.xlsx', index_col=None, dtype=data_types) #, parse_dates=['Date'], date_format='%Y-%m-%d'
    print(table_dfs)

    # Latest record in data set
    max_date = table_dfs['Date'].max()
    # max_date = datetime.strptime(max_date, "%Y-%m-%d")

    new_date = max_date + timedelta(days=1)
    print(max_date.date())
    print(new_date.date())

else:
    print(f"The Excel file '{file_path}' does not exist.")

    # Define column names
    columns = ['Date', 'Task', 'Url', 'ToolsName', 'Url_name']

    # Initialize an empty DataFrame with columns
    df_tasks = pd.DataFrame(columns=columns)



def cleanName (name):
    '''Clean AI names with prefix'''
    if name.startswith('· '):
        return name.replace('· ','')
    else :
        return name

def createSiteUrlName(name):

    return name.strip().replace('. ', '-').replace(' - ', '-').replace(' ','-').replace('.','-').replace('?','')

def createImageName(name):
    return str(name)+".svg"

def create_dir():
    # Directory
    dir_ = "images"
    # Parent Directory path
    parent_dir = os.path.dirname(os.path.realpath(__file__))
    # Path
    path = os.path.join(parent_dir, dir_)
    os.mkdir(path)
    return path

def download_image(name):
    urlName = createSiteUrlName(name)
    image_url = f'https://media.theresanaiforthat.com/icons/{urlName}.svg'
    fullname = createImageName(urlName)
    urllib3.request.urlretrieve(image_url,fullname)


    # img_data = requests.get(url=image_url, headers=headers).content
    # with open(create_dir() + "/" + 'image_name' + '.png', 'wb') as handler:
    #     handler.write(img_data)


# Navigate to website to scrape
driver.get("https://theresanaiforthat.com/timeline/")

# Find all tables on the webpage
# tasks = driver.find_elements(By.CLASS_NAME, "task")

soup = BeautifulSoup(driver.page_source, features="html.parser")


tasks  = soup.find_all("div", {"class": "task"})





for task in tasks :

    Att_Date = task.find("span", {"class": "task_date"}).text
    Att_Task = task.find("span", {"class": "details"}).find("a", {"class": "ai_link task_name"}).text
    Att_Url = task.find("span", {"class": "details"}).find("a", {"class": "ai_link domain"}, href=True)['href']
    Att_ToolsName = cleanName(task.find("span", {"class": "details"}).find("a", {"class": "ai_link domain"}).text)
    ATT_UrlName = createSiteUrlName(Att_ToolsName)
    d = {'Date':Att_Date, 'Task':Att_Task,'Url':Att_Url,'ToolsName':Att_ToolsName , 'Url_name':ATT_UrlName}
    df_tasks.loc[len(df_tasks)]=d


    # if os.path.isfile(createImageName(createSiteUrlName(Att_ToolsName))):
    #     pass
    # else:
    #     try :
    #         download_image(Att_ToolsName)
    #     except :
    #         print(Att_ToolsName)

df_tasks.to_excel('df_tasks.xlsx')
# Close webdriver
driver.quit()





