from bs4 import BeautifulSoup
from urllib.parse import urlparse
import pandas as pd
import time
import re

def soupParser(driver):
  '''return page source by BeautifulSoup'''

  soup = BeautifulSoup(driver.page_source, features="html.parser")

  return soup

def createCatUrl(baseUrl, cat):
  '''Return category url'''

  catUrl = [baseUrl, "/?category=",cat.replace(' ','-')]

  return ''.join(catUrl)


def findCategories(driver):
  '''Return all categories inside the site'''

  soup = soupParser(driver)
  allinputs = soup.find_all('input', {'name': 'category_radio'})
  allCat = []

  for i in allinputs:
    val = i['value']
    allCat.append(val)

  return allCat



def allAvailableAi(driver, cat, baseUrl):
  '''All existing ai in category'''

  columns = ['cat', 'url_internal']
  ais = pd.DataFrame(columns=columns)

  url = createCatUrl(baseUrl, cat)

  driver.get(url)
  time.sleep(5)
  soup = soupParser(driver)
  allcomps = soup.select_one("div.jsx-94a67d70016076c7.grid.grid-cols-1.sm\:grid-cols-2.lg\:grid-cols-3.gap-6.lg\:gap-12.py-1.px-2.justify-center.items-center.text-center")
  allAis = allcomps.find_all("div",  class_=re.compile("relative | bg-foreground | shadow-sm | md:shadow-none | lg:p-0 | md:border-[1px] | border-borderColor | mb-3 | rounded-lg "))

  for i in allAis :

    aiUrlTag=  i.find("a", href=True)
    aiUrl = aiUrlTag['href']
    row = [cat,aiUrl]
    index = ais.shape[0] + 1
    ais.loc[index] = row

  return ais


def fullAis(driver, categories, URL_TARGET):
  '''Find all Ais in all existing categories'''

  columns = ['cat', 'url_internal']
  fullais = pd.DataFrame(columns=columns)


  for i in categories:

    ais = allAvailableAi(driver, i, URL_TARGET)

    fullais = pd.concat([fullais,ais])

  return fullais


#----------------- Private page extraction modules
def extractDescTable(soup):
  '''Extract all information data in table'''
    # Find the table containing the exchange rate data
  table = soup.find('table', {'class': re.compile("text-[15px] | text-general_text | border-spacing-y-[6px] | border-separate | mt-4")})

  if table:
      # Find all rows in the table
      rows = table.find_all('tr')

      # Initialize a dict to store the data
      data = dict()

      # Iterate through the rows and extract the data
      for row in rows:  # Skip the header row
          columns = row.find_all('td')
          des = columns[0].text.strip().replace(":", "").replace(" ", "_").lower()
          det = columns[1].text.strip()
          data.update({des:det})
      return data
      # Print the extracted data

  else:
      print("Table not found on the page.")


def pureURL(soup):
  '''Extract cleaned url of target AI'''

  try:
    url = soup.select_one("div.bg-foreground.rounded-lg > div.md\\:flex > div.basis-\\[46\\%\\].px-4.md\\:pr-6.py-3.pt-5.pb-8.rounded-lg.min-h-\\[410px\\] > div.flex > a")
    url = url['href']
    pureURL = urlparse(url).netloc
  except:
     print("Url not found")
     return None

  return pureURL

def extractShortDescription(soup):
  '''Extreact short description of AI'''
  try:
    desc = soup.select_one("div.bg-foreground.rounded-lg > div.md\\:flex > div.basis-\\[46\\%\\].px-4.md\\:pr-6.py-3.pt-5.pb-8.rounded-lg.min-h-\\[410px\\] > p").text
  except:
     print("description not found")
     return None

  return desc


def realeaseYear(soup):
  '''Extract realease year'''

  try:
    year = soup.select_one("div.bg-foreground.rounded-lg > div.md\\:flex > div.basis-\\[46\\%\\].px-4.md\\:pr-6.py-3.pt-5.pb-8.rounded-lg.min-h-\\[410px\\] > div.flex > a > span").text
    try :
      year = int(year)
    except :
      pass
  except:
    print("year not found")
    return None

  return year


def extractLike(soup):
  '''Extract likes'''
  try:
    likes = soup.select_one("div.bg-foreground.rounded-lg > div.md\\:flex > div.basis-\\[46\\%\\].px-4.md\\:pr-6.py-3.pt-5.pb-8.rounded-lg.min-h-\\[410px\\] > div.flex > div > div > span").text
    try :
      likes = int(likes)
    except :
      likes = 0

  except:
    print("likes not found")
    return None

  return likes


def extractName(soup):
  '''Extract Ai's name'''
  try:
     name = soup.select_one("div.bg-foreground.rounded-lg > div.md\\:flex > div.basis-\\[46\\%\\].px-4.md\\:pr-6.py-3.pt-5.pb-8.rounded-lg.min-h-\\[410px\\] > div.flex > a > h1").text
  except:
     print("Name not found")
     return None

  return name


def extractTags(soup):
  '''Extract Tags'''
  try:
    tags = soup.select_one("div.bg-foreground.rounded-lg > div.md\\:flex > div.basis-\\[46\\%\\].px-4.md\\:pr-6.py-3.pt-5.pb-8.rounded-lg.min-h-\\[410px\\] > div:nth-child(5) > div.text-\\[15px\\].text-general_text.mt-2 > a").text
  except:
     print("Tags not found")
     return None
  return tags


def extractFeturesTables(soup):
  '''Extract all features data in tables'''

    # Find the table containing the exchange rate data
  tables = soup.find_all('ul', {'class': "list-disc"})
  headers = ['Features','Use Cases']
  if tables:

      # Initialize a dict to store the data
      data = dict()

      for i in range(len(tables)):

        # Find all rows in the table
        rows = tables[i].find_all('li')

        # Initialize a dict to store internal data
        internal = dict()

        # Iterate through the rows and extract the data
        for row in rows:  # Skip the header row
            columns = row.find_all('span')
            des = columns[0].text.strip()
            det = columns[1].text.strip()
            internal.update({des:det})
        hed = headers[i]
        data.update({hed:internal})
      return data

  else:
      print("Table not found on the page.")
      return None


def extractScreenShotUrl(soup, URL_TARGET):
  '''Extract screenshot token by AiToolHunt'''

  try:
    screenshot = soup.select_one("div.bg-foreground.rounded-lg > div.md\\:flex > div.md\\:w-\\[640px\\].md\\:mr-4.basis-\\[54\\%\\].rounded-md.p-4.md\\:p-6")
    if screenshot:

      frame = screenshot.find_all('iframe')
      if frame:
          ssUrl = frame[0]['src']
          return ssUrl

      img = screenshot.find_all('img')

      if img[0]:
        ss = img[0]['src']
        ssUrl = [URL_TARGET,ss]
        ssUrl = ''.join(ssUrl)

        return ssUrl
  except:
     print("Screen shots not found")
     return None



def aiFeatures (driver, URL_TARGET, ai):
    '''Gather all information of an Ai in a private page of AitooHunt'''

    print(ai['url_internal'])

    aiPrivateUrl = [URL_TARGET,ai['url_internal']]
    aiPrivateUrl = ''.join(aiPrivateUrl)

    driver.get(aiPrivateUrl)
    time.sleep(3)
    soup = soupParser(driver)

    url = pureURL(soup)
    description = extractShortDescription (soup)
    det = extractDescTable(soup)
    year = realeaseYear(soup)
    like = extractLike(soup)
    name = extractName(soup)
    tags = extractTags(soup)
    ssUrl = extractScreenShotUrl(soup, URL_TARGET)

    features = extractFeturesTables(soup)
    if features:
      for key, value in features.items():
        key = key.strip().lower().replace(" ", "_")
        det.update({key:str([(k,v) for k,v in value.items()])})
    try:

      det.update({
                    'cat':ai['cat'],
                    'url_internal': ai['url_internal'],
                    'url_ai':url,
                    'description':description,
                    'year':year,
                    'likes':like,
                    'name':name,
                    'tags':tags,
                    'url_screen_shot':ssUrl
                })

      det =  pd.DataFrame(det, index=[0])
      return det
      # details = pd.concat([details, det], ignore_index = True, axis = 0)
    except:
      print("No data")
      return None






