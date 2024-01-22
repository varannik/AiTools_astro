'''
Create new selenium remote driver
'''
from selenium import webdriver

def createDriver(URL, URL_SELENIUM, enableCookies=False):
  '''Return driver'''

  options = webdriver.ChromeOptions()
  if enableCookies:
    options.add_experimental_option("prefs", {"profile.default_content_setting_values.cookies": 2})
  driver = webdriver.Remote(command_executor=URL_SELENIUM, options=options)

  driver.get(URL)

  return driver



