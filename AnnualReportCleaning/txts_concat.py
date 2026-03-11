# -*- coding: utf-8 -*-
import os
artxt_path = 'AnnualReportsTXT' #年报文本所在文件夹
for root, dirs, files in os.walk(artxt_path):
    if (len(files) == 0): #跳过没有文件的文件夹
        continue
    for file in files:
        fname, ext = os.path.splitext(file) #获取文件名的无后缀部分
        if (fname.count('-') != 1):#若文件不分part，跳过
            continue
        fpath = "{}\\{}".format(root,file) #拼出文件的完整相对路径
        if (not os.path.exists(fpath)):#若文件不存在，跳过
            continue
        parts = fname.split('-')
        parts.pop()#去除最后的part部分
        newfname = '-'.join(parts)
        while '' in parts:
            parts.remove('')
        newfname_final = '-'.join(parts) #防止生成18-.txt这样的文件名
        newfpath = "{}\\{}{}".format(root,newfname,ext) #拼出新文件的完整相对路径
        newfpath_final = "{}\\{}{}".format(root,newfname_final,ext) #拼出新文件的完整相对路径
        new_content = []
        i = 0
        while True:
            i += 1
            partfpath = "{}\\{}-{}{}".format(root,newfname,i,ext) #拼出part文件的完整相对路径
            if (not os.path.exists(partfpath)):#若文件不存在，跳过
                break
            with open(partfpath,mode='r') as f: #打开txt文件并读取内容
                new_content.append(f.read())
            os.remove(partfpath)
        with open(newfpath_final,mode='w') as f: #打开txt文件并开始写入/覆盖
            f.write("\n".join(new_content))
        print("Done {}".format(newfpath)) #完成提取，输出新文件位置提示