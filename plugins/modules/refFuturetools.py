from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin
import pandas as pd
from globalElements import *
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
    data.update({'url_ai': pageUrl , 'url_screen_shot': screenshotUrl , 'short_description': shortDes, 'name':nameRef , 'url_internal':priveUrl})
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


def extName(soup):
    '''Clean AI name'''
    try:
      name = soup.select_one("#changemode > div.section-2 > div.div-block-5 > div > h1").text
      return name

    except:
       print('No name found')
       return None


def extDesciption(soup):
    '''Extract description'''
    try:
        des = soup.select_one("#w-node-_0d11088c-1bcb-2225-b918-232ca35a6aff-799ae876 > div.rich-text-block.w-richtext").text

        return des
    except:
        print("No description found")
        return None

def extPrice(soup):
    '''Extract Pricing Model'''
    try:
        price = soup.select_one("#w-node-_0d11088c-1bcb-2225-b918-232ca35a6aff-799ae876 > div.div-block-17 > div:nth-child(2)").text

        return price
    except:
        print("No description found")
        return None

def extTags(soup):
    '''Extract tags'''
    try:
        tagList = []
        tags = soup.select_one("#w-node-_0d11088c-1bcb-2225-b918-232ca35a6aff-799ae876 > div.tags-block > div.collection-list-wrapper-8.w-dyn-list > div")
        for tag in tags:
          t = tag.select_one(" a > div").text
          tagList.append(t)

        return tagList
    except:
        print("No description found")
        return None

def extRedicrectedUrl(soup):
    '''Extract Pricing Model'''
    try:
        url = soup.select_one("#w-node-_0d11088c-1bcb-2225-b918-232ca35a6aff-799ae876 > div.div-block-6.vertical-flex > a")
        url= url['href']

        return url
    except:
        print("No Redirected url found")
        return None

def extUpVote(soup):
    '''Extract Up vote'''
    try:
        vote = soup.select_one("#w-node-_0d11088c-1bcb-2225-b918-232ca35a6afe-799ae876 > div > div.not-upvoted > a > div").text


        return int(vote)
    except:
        print("No UpVote found")
        return None


def extScreenShotUrl(soup):
    '''Extract screenshot url'''
    try:
        img = soup.select_one("#w-node-_0d11088c-1bcb-2225-b918-232ca35a6afe-799ae876 > a > img")
        img = img['src']
        print(img)
        return img
    except:
        print("No img found")
        return None

def aiFeatures(driver, URL_TARGET, ai):
    '''Gather all information of an Ai in a private page of FutureAi'''

    aiPrivateUrl = [URL_TARGET,ai['url_internal']]
    aiPrivateUrl = ''.join(aiPrivateUrl)

    driver.get(aiPrivateUrl)
    time.sleep(3)
    soup = soupParser(driver)

    name = extName(soup)
    description = extDesciption(soup)
    price = extPrice(soup)
    tags = extTags(soup)
    urlRed = extRedicrectedUrl(soup)
    vote = extUpVote(soup)
    url_screen_shot = extScreenShotUrl(soup)


    # try:

    det =dict()
    df = pd.DataFrame(columns=['name', 'description', 'url_internal', 'pricing', 'url_ai', 'up_vote', 'tags', 'url_screen_shot'])

    det.update({
        'name':name,
        'description':description,
        'url_internal': ai['url_internal'],
        'pricing': price,
        'url_ai':urlRed,
        'up_vote': vote,
        'tags':tags,
        'url_screen_shot':url_screen_shot
        })

    df.loc[0]=det
    return df

    # except:
    #   print("No data")
    #   return None

#----------------- Target page features
def targetPageFeatures(driver, row):
    '''Extract all information from target Ai page '''

    url = row['url_ai']
    name = row['url_ai'].replace('.','_')
    print(url)

    if 'http' in url:
        try:
            driver.get(url)
        except:
            pass

    else:
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

    currentUrl = "http://" + urlparse(driver.current_url).netloc

    soup = soupParser(driver)

    #------------- favicon
    favicon = getFavicon(soup)
    if not 'http' in favicon:
        favicon = urljoin(currentUrl ,favicon)

    #------------- logo
    logo = cleanLogoStr(soup, currentUrl)
    try:
        if len(logo)>1000:
            logo=None
    except:
        pass

    #------------- screenshot
    screenPath = takeScreenShot(driver,name)

    data = dict()
    data.update({
        "url_ai": row['url_ai'],
        "url_stb" :currentUrl,
        "url_fav" :favicon,
        "url_log":logo,
        "path_screen_shot":screenPath
    })
    print(data)
    data =  pd.DataFrame(data, index=[0])

    return data