"""
Modules inside extraction data from Theresanaiforthat
"""
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import pandas as pd
import os
import re


def soupParser(driver):
  '''return page source by BeautifulSoup'''

  soup = BeautifulSoup(driver.page_source, features="html.parser")

  return soup

def cleanName(task):
    '''Clean AI names with prefix'''

    name = task.find("span", {"class": "details"}).find("a", {"class": "ai_link domain"}).text
    if name.startswith('· '):
        return name.replace('· ','')
    else :
        return name

def createSiteUrlName(name):

    return name.strip() \
            .replace('. ', '-') \
            .replace(' - ', '-') \
            .replace(' ','-') \
            .replace('.','-') \
            .replace('?','') \
            .replace("'", "-") \
            .replace("(", "") \
            .replace(")", "") \
            .replace(":", "") \
            .replace("/", "-") \
            .replace("@", "-") \
            .replace("-|-", "-") \
            .replace("!-", "")


def taskName(task):

    return  task.find("span", {"class": "details"}).find("a", {"class": "ai_link task_name"}).text


def reldate(task):

    return task.find("span", {"class": "task_date"}).text


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

def extPureURLTimeLine(task):
  '''Extract cleaned url of target AI'''

  try:
    url = task.find("span", {"class": "details"}).find("a", {"class": "ai_link domain"}, href=True)
    url = url['href']
    pureURL = urlparse(url).netloc
  except:
     print("Url not found")
     return None

  return pureURL


def fullAis(driver):
    '''Find all Ais in timeline'''

    # Parse the HTML using Beautiful Soup
    soup = soupParser(driver)

    tasks  = soup.find_all("div", {"class": "task"})

    # Initialize an empty DataFrame with columns
    columns = ['name', 'task', 'url_internal', 'url_ai', 'rel_date']
    df_tasks = pd.DataFrame(columns=columns)

    for task in tasks :

        name = cleanName(task)
        taskN = taskName(task)
        url_internal = createSiteUrlName(name)
        url_ai = extPureURLTimeLine(task)
        rel_date = reldate(task)

        d = {'name': name, 'task':taskN, 'url_internal':url_internal, 'url_ai':url_ai, 'rel_date': rel_date}
        df_tasks.loc[len(df_tasks)]=d

    return df_tasks

#----------------- All tasks

def findAllTasks(driver):
    '''Find all tasks exist in TheresAnAIForThat'''
    # Parse the HTML using Beautiful Soup
    soup = soupParser(driver)

    try:
        taskList = []
        al = soup.select_one("#main > div.desk_row > div.data_cont.request-fade-in-top > div.li_cont > ul")
        al = al.find_all('a')
        for a in al:
            li = a['href']
            taskList.append(li)

        print(f"{len(taskList)} tasks found")
        return taskList

    except:
        print('Thare is no task')


def cleanStr(str):
    rx = re.compile('\W+')
    res = rx.sub(' ', str).strip()
    return res


def discoverTasks(driver, task, ):
    '''Explore each task category and find all AIs'''

    URL_TARGET='https://theresanaiforthat.com'
    urlInternal = [URL_TARGET,task]
    urlInternal = ''.join(urlInternal)
    print(urlInternal)

    # Open task page
    driver.get(urlInternal)

    soup = soupParser(driver)

    # Extract all exist AIs
    ais = pd.DataFrame()

    allAis = soup.select_one("div.desk_row > div.data_cont.request-fade-in-top > div.li_cont > ul")
    if allAis:
      allAis = allAis.find_all('div', {'class':'ai_link_wrap'})

      for x in allAis:
        l = x.select_one("a.ai_link.new_tab")
        l = l['href']
        n  = x.select_one("a.ai_link.new_tab > span").text


        tks = dict()
        tks.update({'task':task, 'name': n ,'url_internal':l})

        det =  pd.DataFrame(tks, index=[0])
        ais = pd.concat([ais, det], ignore_index = True, axis = 0)

    print(f"{len(ais)} task and Ai found")
    return ais

#----------------- Private page extraction modules

def extRate(soup):
    ''' Extract total rate of Ai'''

    try:
      r = soup.select_one("div.left > div.column > span > a > span:nth-child(2)").text

      return float(r)

    except:
      print("No Rate found")
      return None


def extCountRate(soup):
    '''Extract count rates'''
    try:
      cr = soup.select_one("div.left > div.column > span > a.rating_top > span.ratings_count").text
      cr = int(''.join(filter(str.isdigit, cr)))

      return cr
    except:
      print("No Count Rate found")
      return None

def extSaves(soup):
    '''Extract count saves'''

    try:
      sa = soup.select_one("div.left > div.column > span > div").text
      sa = int(''.join(filter(str.isdigit, sa)))

      return sa

    except:
      print("No Count saves found")
      return None


def extComments(soup):
    '''Extract count comments'''
    try:
      co = soup.select_one("div.left > div.column > span > a.comments").text
      co = int(''.join(filter(str.isdigit, co)))

      return co

    except:
      print("No Count comments found")
      return None


def extAiURL(soup):
  '''Extract cleaned url of target AI'''

  try:
    url = soup.select_one("#ai_top_link")
    url = url['href']

    return url

  except:
     print("Url not found")
     return None




def extUseCase(soup):
    '''Extract use cases of Ai'''
    try:
      us = soup.select_one("#use_case").text

      return us

    except:
      print("No usecase found")
      return None


def extTags(soup):
    '''Extract tags as a list'''
    try:
      tgs =[]
      allTagDivs = soup.select_one("#rank > div.tags")
      allTagDivs = allTagDivs.find_all("a", {"class":"tag"})

      for tag in allTagDivs:
          tgs.append(tag.text)

      return tgs

    except:
      print("No tag found")
      return None


def extPrice(soup):
   '''Extract Price of Ai'''

   try:
      pr = soup.select_one("#rank > div.tags > span").text

      return pr

   except:
      print("No price found")
      return None


def extScreenShot(soup):
    '''Extract screen shot url taken by theresanaiforthat'''

    try:
      url = soup.find('img', {"class":"ai_image"})
      url = url['src']

      return url

    except:
      print("No screen shot")
      return None



def extdesc(soup, url_internal):
    '''Extract description'''

    try:
      des = pd.DataFrame(columns=['url_internal', 'descrption'])
      ds = soup.select_one("#data_cont > div.rright > div > div.description")
      ds = ds.find_all("p")
      for d in ds:
        det = dict()

        det.update({
                  'url_internal':url_internal,
                  'descrption'  : d.text
                  })
        det =  pd.DataFrame(det, index=[0])
        des  = pd.concat([des, det], ignore_index = True, axis = 0)

      return des
    except:
        print("No description found")
        return None


def extFeatures(soup, url_internal):
    '''Extract all properties form private pages'''

    try:
      res = pd.DataFrame(columns=[
      'url_internal',
      'rate',
      'count_rate',
      'count_save',
      'count_comments',
      'usecase',
      'tags',
      'price',
      'url_screen_shot',
      'url_ai'
                                   ])

      det = dict()

      det.update(
      {
          'url_internal' : url_internal,
          'rate'         : extRate(soup),
          'count_rate'   : extCountRate(soup),
          'count_save'   : extSaves(soup),
          'count_comments':extComments(soup),
          'usecase'      : extUseCase(soup),
          'tags'         : extTags(soup),
          'price'        : extPrice(soup),
          'url_screen_shot': extScreenShot(soup),
          'url_ai'       : extAiURL(soup)
      }
      )
      res.loc[0]=det
      return res

    except:
      print('No properties found')
      return None


def extMostImpacedJobs(soup, url_internal):
    '''Extract most impacted jobs and their ratios'''
    def ns(st):
       return int(''.join(filter(str.isdigit, st)))

    try:

      jds =dict()
      relatedJobs = pd.DataFrame(columns=['title', 'impact', 'count_tasks', 'count_ais'])

      ijs = soup.select_one("#most_impacted_jobs")
      ijs = ijs.find_all('a')

      for ij in ijs:

        j = ij.find('span', {"class":"related_job_name"}).text
        i = ij.find('span', {"class":"related_impact"}).text
        t = ij.find('span', {"class":"related_tasks"}).text
        a = ij.find('span', {"class":"related_ais"}).text

        jds.update({'url_internal':url_internal, 'title':j, 'impact':ns(i), 'count_tasks':ns(t), 'count_ais':ns(a)})

        det =  pd.DataFrame(jds, index=[0])
        relatedJobs  = pd.concat([relatedJobs, det], ignore_index = True, axis = 0)

      return relatedJobs

    except:
       print("No Impacted job found")
       return None

def extAtts(soup, url_internal):
  '''Extract attributes consist of pros and cons'''

  try:
    att = pd.DataFrame(columns=['url_internal', 'attribute', 'description'])

    try:
      allpros = soup.select_one("#pros-and-cons > div > div.pac-info-item.pac-info-item-pros")
      allpros = allpros.find_all('div')
      for i in allpros:
        att.loc[len(att)] = [url_internal, 'pros', i.text]
    except:
      print("No pros job found")

    try:
      allpros = soup.select_one("#pros-and-cons > div > div.pac-info-item.pac-info-item-cons")
      allpros = allpros.find_all('div')
      for i in allpros:
        att.loc[len(att)] = [url_internal, 'cons', i.text]

    except:
      print("No cons job found")

    print(f"{len(att)} attribute found")
    return att

  except:
     print("No attribute found")
     return None



