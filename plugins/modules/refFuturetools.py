from bs4 import BeautifulSoup
from urllib.parse import urlparse
import pandas as pd
import time
import re

def soupParser(driver):
  '''return page source by BeautifulSoup'''

  soup = BeautifulSoup(driver.page_source, features="html.parser")

  return soup

def createCatUrl(baseUrl, pv):
  '''Return category url'''

  catUrl = [baseUrl, pv]

  return ''.join(catUrl)


def topLevelDet(el):

    data = dict()

    pageUrl = el.select_one("div.tool-item-coliumn-2---new.w-col.w-col-7.w-col-medium-7.w-col-small-7.w-col-tiny-6 > div > div.tool-item-text-link-block---new.flex-center > a.tool-item-new-window---new.w-inline-block")
    pageUrl = pageUrl['href']

    screenshotUrl = el.select_one("div.tool-item-column-1---new.w-col.w-col-5.w-col-medium-5.w-col-small-5.w-col-tiny-6 > a > img")
    screenshotUrl = screenshotUrl['src']

    name = el.select_one("div.tool-item-coliumn-2---new.w-col.w-col-7.w-col-medium-7.w-col-small-7.w-col-tiny-6 > div > div.tool-item-text-link-block---new.flex-center > a.tool-item-link---new")
    nameRef = name.text
    priveUrl = name['href']
    shortDes = el.select_one("div.tool-item-coliumn-2---new.w-col.w-col-7.w-col-medium-7.w-col-small-7.w-col-tiny-6 > div > div.tool-item-description-box---new").text
    data.update({'pageUrl': pageUrl , 'screenshotUrl': screenshotUrl , 'shortDes': shortDes, 'name':nameRef , 'priveUrl':priveUrl})
    df =  pd.DataFrame(data, index=[0])
    return df


def allAis(driver):

  # Parse the HTML using Beautiful Soup
  soup = soupParser(driver)

  allAis = soup.find_all("div",  class_=re.compile("tool-item-columns-new | w-row") )

  details = pd.DataFrame()

  for i in allAis:
    try :

      df = topLevelDet(i)
      details = pd.concat([details, df], ignore_index = True, axis = 0)
      details.to_excel("futureAi.xlsx")

    except:
      pass
  return details


def scroll_down_to_load(driver):
    """A method for scrolling the page."""

    # Get scroll height.
    last_height = driver.execute_script("return document.body.scrollHeight")


    while True:
      # Scroll down to the bottom.
      driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")


      soup = soupParser(driver)
      count = soup.find_all("div",  class_=re.compile("tool-item-columns-new | w-row") )
      print("Count element found", len(count))


      if len(count) < 2000:
        driver.execute_script("window.scrollTo(0, 0);")
      else:
        # Calculate new scroll height and compare with last scroll height.
        new_height = driver.execute_script("return document.body.scrollHeight")

        if new_height == last_height:
            print("waiting loop")
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight - 1300);")
            # Wait to load the page.
            time.sleep(10)
            new_height = driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
              break

        last_height = new_height

    return allAis(driver)








