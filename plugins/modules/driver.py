'''
Create new selenium remote driver
'''
from selenium import webdriver

def createDriver(URL, URL_SELENIUM):
  '''Return driver'''

  options = webdriver.ChromeOptions()
  driver = webdriver.Remote(command_executor=URL_SELENIUM, options=options)
  driver.get(URL)

  return driver

