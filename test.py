from urllib import request

#response = request.urlopen(r'http://stock.10jqka.com.cn/fhspxx_list/')
response = request.urlopen(r'http://stock.10jqka.com.cn/20170602/c598751405.shtml')

page = response.read()
print(page)