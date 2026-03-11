# -*- coding: utf-8 -*-
import pdfplumber, os
arpdf_path = 'AnnualReportsPDF' #年报所在文件夹
artxt_path = 'AnnualReportsTXT' #年报文本所在文件夹
for root, dirs, files in os.walk(arpdf_path):
    if (len(files) == 0): #跳过没有文件的文件夹
        continue
    new_root = root.replace(arpdf_path, artxt_path, 1) #根路径替换
    for file in files:
        fname = os.path.splitext(file)[0] #获取文件名的无后缀部分
        fpath = "{}\\{}".format(root,file) #拼出pdf文件的完整相对路径
        new_fpath = "{}\\{}.txt".format(new_root, fname) #txt文件的完整相对路径
        pdf = pdfplumber.open(fpath, password='')
        new_content = []
        if not os.path.exists(new_root): #若目录不存在，则创建文件夹
            os.makedirs(new_root)
        for page in pdf.pages: #遍历pdf的每一页提取文本，不保持排版layout=False
            new_content.append(page.extract_text(layout=False, use_text_flow=True))
        with open(new_fpath,mode='w') as f: #打开txt文件并开始写入/覆盖
            f.write("\n".join(new_content))
        print("Done {}".format(new_fpath)) #完成提取，输出新文件位置提示