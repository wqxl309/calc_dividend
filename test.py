from gmsdk import md

md.init('18201141877','Wqxl7309')

dbar = md.get_dailybars('CFFEX.IC1707','20170620','20170620')

print(dbar)