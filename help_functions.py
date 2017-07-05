import csv

def ls1_filter(infile,outfile):
    """ 过滤百泉砺石的持仓中的单位字符 """
    checks = ['当前拥股','冻结数量','可用数量','在途股份','分级可合并分拆量','分级可赎回量','ETF申赎可用量']
    with open(outfile,'w',newline='') as outfl:
        writer = csv.writer(outfl)
        titlefound = False
        with open(infile,'r') as infl:
            reader = csv.reader(infl)
            for line in reader:
                if not titlefound:
                    if '证券名称' in line:
                        titles = line
                        checkpos = [ titles.index(ck) for ck in checks ]
                        titlefound = True
                        writer.writerow(titles)
                elif titlefound:
                    for dumi in range(len(line)):
                        if dumi in checkpos:
                            line[dumi] = line[dumi].strip('股')
                            line[dumi] = line[dumi].strip('张')
                    writer.writerow(line)
                else:
                    raise Exception('No title found !')