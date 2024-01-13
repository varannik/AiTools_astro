''''
Extract special global elemnts (Logo, Favicon, ...)
'''
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
import pandas as pd
import os

path = f'{os.getcwd()}/plugins/modules/screenshots'

if os.path.isdir(path):
    os.chdir(path)
else:
    os.mkdir(path)
    os.chdir(path)


def soupParser(driver):
  '''return page source by BeautifulSoup'''

  soup = BeautifulSoup(driver.page_source, features="html.parser")

  return soup


def getFavicon(soup):
  ''' Extract favicon from source html'''

  icon_link = soup.find("link", rel="shortcut icon")
  if icon_link is None:
      icon_link = soup.find("link", rel="icon")
  if icon_link is None:
      return '/favicon.ico'
  return icon_link["href"]



def opt1(soup):

    img_tags = soup.find_all('img')

    for img_tag in img_tags:
      d = img_tag.attrs
      for key, value in d.items():
        if isinstance(value, list):
           value = value[0]
        if "logo" in value.lower():
          if any(x in value.lower() for x in ['jpg', 'png', 'svg']):
            return value

def opt2(soup):

    img_tags = soup.find_all('img')
    try :
      return img_tags[0].get('src')
    except:
       pass
    try:
       return img_tags[0].get('srcset')
    except:
       print("No Source")


def getLogoStr(soup):
  '''Extract possible existing logo urls'''

  try :

    if opt1(soup):
        return opt1(soup)

    else :
        return opt2(soup)

  except :
     return None

  # except:
  #       print("No logo found")

def cleanLogoStr(soup, URL_TARGET):
  '''Return cleaned logo url '''
  try:
    head, sep, tail = getLogoStr(soup).partition(' ')
    if "http" in head:
      return head
    else:
      return urljoin(URL_TARGET, head)
  except:
     return None



def takeScreenShot(driver, name):
  '''Take screenShot of target Ai web page'''
  print(os.getcwd())

  namePath = f'{name}_scr.png'
  excPath = f'{os.getcwd()}/plugins/modules/screenshots/{name}_scr.png'
  print(excPath)
  driver.save_screenshot(excPath)

  return namePath



#----------------- Target page features

def targetPageFeatures(driver, row):
  '''Extract all information from target Ai page '''

  driver.set_page_load_timeout(10)
  # details = pd.DataFrame()

  # for index, row in Ais.iterrows():

  url = row['url_ai']
  name = row['url_ai'].replace('.','_')
  print(url)

  re_url = url

  try :
    re_url = "https://" + url
    print(re_url)
    driver.get(re_url)
    url = re_url

  except:
    try:
      re_url = "https://www." + url
      print(re_url)
      driver.get(re_url)
      url = re_url
    except:
      re_url = "http://www." + url
      print(re_url)
      driver.get(re_url)
      url = re_url

  time.sleep(5)

  currentUrl = driver.current_url

  soup = soupParser(driver)

  favicon = getFavicon(soup)
  if not 'http' in favicon:
    favicon = url + favicon


  logo = cleanLogoStr(soup, url)
  try:
    if len(logo)>1000:
        logo=None
  except:
    pass


  screenPath = takeScreenShot(driver,name)

  data = dict()
  data.update({
    "url_ai": row['url_ai'],
    "url_stb" :currentUrl,
    "url_fav" :favicon,
    "url_log":logo,
    "path_screen_shot":screenPath
  })

  data =  pd.DataFrame(data, index=[0])

    #   details = pd.concat([details, data], ignore_index = True, axis = 0)
    # except :
    #   pass

  return data
