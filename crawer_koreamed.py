# coding:utf-8
'''
author:wangyi
'''

import requests
from bs4 import BeautifulSoup
from collections import defaultdict
from tqdm import tqdm
import time
import pandas as pd
import pickle
import os
import argparse

def get_download_result(journalname,volume,issue):
    '''
    获取详情信息
    :param journalname: 期刊名
    :param volume: 期刊号
    :param issue: 卷号
    :return: 指定期刊号和卷号下的所有数据
    '''
    url = 'https://koreamed.org/search/result?q=(journal:%22'+str(journalname)+'%22+AND+volume:'+str(volume)+')+AND+issue:'+str(issue)+'&resultsPerPage=9999&page=1&display=Summary&sort=Date'

    req = requests.get(url)
    resps = req.json()['results']['data']
    #print(url,req.status_code)
    return resps



def output_to_file(writer,r,count):
    '''
    输出文件
    :param writer: IO流
    :param r: 单条记录
    :param count: 当前记录序号
    :return:
    '''
    if 'publishinfo' in r.keys():
        writer.write(str(count) + ':' + r['publishinfo'] + '\n')
    if 'journal_name' in r.keys():
        writer.write('\t'+'journal_name\t-'+str(r['journal_name'])+'\n')
    if 'pissn' in r.keys():
        writer.write('\t' + 'IS\t-' + r['pissn'] + '(Print)' + '\n')
    if 'eissn' in r.keys():
        writer.write('\t' + 'IS\t-' + r['eissn'] + '(Eletronic)' + '\n')
    if 'volume' in r.keys():
        writer.write('\t' + 'VI\t-' + str(r['volume']) + '\n')
    if 'issue' in r.keys():
        writer.write('\t' + 'IP\t-' + str(r['issue']) + '\n')
    if 'title' in r.keys():
        writer.write('\t' + 'TI\t-' + r['title'] + '\n')
    if 'publishinfo' in r.keys():
        writer.write('\t' + 'DP\t-' + r['publishinfo'].split('.')[1].split(';')[0] + '\n')
        writer.write('\t' + 'PG\t-' + r['publishinfo'].split('.')[1].split(';')[1].split(':')[1] + '\n')
    if 'doi' in r.keys():
        writer.write('\t' + 'DOI\t-' + r['doi'] + '\n')
    if 'abstract' in r.keys():
        writer.write('\t' + 'AB\t-' + r['abstract'] + '\n')
    if 'author_facet' in r.keys():
        for i in range(len(r['author_facet'])):
            writer.write('\t' + 'FAU\t-' + r['author_facet'][i] + '\n')
            writer.write('\t' + 'AU\t-' + r['author_initial'][i] + '\n')
    if 'affiliate_facet' in r.keys():
        for i in range(len(r['affiliate_facet'])):
            writer.write('\t' + 'AD\t-' + r['affiliate_facet'][i] + '\n')
    if 'language' in r.keys():
        writer.write('\t' + 'LA\t-' + r['language'] + '\n')
    if 'article_type' in r.keys():
        writer.write('\t' + 'PT\t-' + r['article_type'] + '\n')
    if 'mesh' in r.keys():
        for i in range(len(r['mesh'])):
            writer.write('\t' + 'MH\t-' + r['mesh'][i] + '\n')
    if 'author_keyword' in r.keys():
        for i in range(len(r['author_keyword'])):
            writer.write('\t' + 'KW\t-' + r['author_keyword'][i] + '\n')
    if 'journal_id_nlm_ta' in r.keys():
        writer.write('\t' + 'TA\t-' + r['journal_id_nlm_ta'] + '\n')
    if 'accepted_date' in r.keys():
        writer.write('\t' + 'DE\t-' + r['accepted_date'] + '\n')
    if 'id' in r.keys():
        writer.write('\t' + 'KUID\t-' + r['id'] + '\n')
    if 'doi' in r.keys():
        writer.write('\t' + 'AID\t-' + r['doi'] + '[doi]\n')
    if 'publishinfo' in r.keys():
        writer.write('\t' + 'SO\t-' + r['publishinfo'] + '\n')
    writer.write('\n')
    writer.flush()



def get_journalname_volume():
    '''
    获取所有期刊名和期号
    :return:
    '''
    requests.adapters.DEFAULT_RETRIES = 5
    # 获取期刊列表的所有期刊名称及其对应网页跳转id
    source_src = requests.get('https://koreamed.org/journals')
    soup = BeautifulSoup(source_src.text,'lxml')
    journallinks = soup.findAll(name="a", attrs={"class" :"DefaultJournalItemLink"})
    journalnames = soup.findAll(name="span", attrs={"class" :"journalsubinfo"})
    journalname2page = {}
    qc2sx  = {}
    for i,item in enumerate(journalnames):
        journalname2page[item.text.split('|')[0].strip()] = journallinks[2*i]['href'].split('/')[1].strip()

        qc2sx[journallinks[2*i].text] = item.text.split('|')[0].strip()
    journalname2volumn = defaultdict(list)
    # 获取每个期刊下的期刊号和卷号
    for k,v in tqdm(journalname2page.items()):
        detail = requests.get('https://koreamed.org/volumes/'+v)
        soup = BeautifulSoup(detail.text,'lxml')
        volums = soup.findAll(name="div", attrs={"class" :"text-md-center"})
        for volum in volums:
            if volum.contents[1]['href'].find(v) != -1:
                m = {'volume':volum.contents[1]['href'].split('/')[-2].strip(),'issue':volum.contents[1]['href'].split('/')[-1].strip()}
                if m not in journalname2volumn[k]:
                    journalname2volumn[k].append(m)
    pickle.dump([journalname2volumn,qc2sx],open('j_info.pkl','wb'))
  


def get_special_data_record(special_excel,sheet_name):
    '''
    在给定信息的excel下获取记录
    :param special_excel:
    :param sheet_name:
    :return:
    '''
    journalname2volumn, qc2sx = pickle.load(open('j_info.pkl','rb'))
    df = pd.read_excel(special_excel,sheetname=sheet_name,header=None)

    result = defaultdict(list)
    error = []
    for i in tqdm(range(df.shape[0])):
        try:
            result[df.iloc[i,0]].extend(get_download_result('+'.join(qc2sx[df.iloc[i,0]].split(' ')),str(df.iloc[i,4]),str(df.iloc[i,5])))
        except:
            error.append([df.iloc[i,0],df.iloc[i,4],df.iloc[i,5]])
            print('no record by:',df.iloc[i,0],'volume:',df.iloc[i,5],'issue:',df.iloc[i,5])
        time.sleep(1)
    pickle.dump([result,error],open('crawer_result.pkl','wb'))



def output_file(special_excel,sheet_name,out_dir):
    result, error = pickle.load(open('crawer_result.pkl','rb'))
    values = []
    for v in result.values():
        values.extend(v)
    df = pd.read_excel(special_excel, sheetname=sheet_name,header=None)
    errorlog = []
    for i in tqdm(range(df.shape[0])):
        c = 1
        file_name = ''
        for j in range(df.shape[1]):
            file_name += str(df.iloc[i,j])
            if j != df.shape[1] - 1:
                file_name += '-'
        writer = open(os.path.join(out_dir,file_name+'.txt'),'w',encoding='utf-8')
        for v in values:
            try:
                if v['journal_name'] == str(df.iloc[i,0])  and v['publication_date_year'] == int(df.iloc[i,3]) and v['volume'] == int(df.iloc[i,4]) and v['issue'] == str(df.iloc[i,5]):
                    output_to_file(writer,v,c)
                    c += 1
            except Exception as e:
                errorlog.append([v,e])
    pickle.dump(errorlog,open(os.path.join(out_dir,'errorlog.pkl'),'wb'))


def main(special_excel,sheet_name,out_dir):

    get_special_data_record(special_excel,sheet_name)

    output_file(special_excel,sheet_name,out_dir)




if __name__ == '__main__':

    parse = argparse.ArgumentParser('crawer')
    parse.add_argument('--special_excel',type=str,default='./20190603期刊数据下载副本.xlsx')
    parse.add_argument('--sheet_name',type=str,default='Sheet1')
    parse.add_argument('--out_dir',type=str,default='./crawer_results')
    args = parse.parse_args()

    main(args.special_excel,args.sheet_name,args.out_dir)
