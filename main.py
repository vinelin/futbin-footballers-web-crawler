# coding = UTF-8
import random

import aiohttp
import asyncio
import lxml.html
import csv
import pandas
import re
import json
from html import unescape

maxPage = None
url_fail = []
player_list = []
proxy = None
csv_head = ['ShortName',
            'NameOnCard',
            'Name',
            'Nation',
            # 2022.6.24 增加球员所在联赛,
            "League",
            'Weak Foot',
            'Intl. Rep',
            'Score',
            'Foot',
            'Height',
            'Weight',
            #2022.7.27 增加球员是否是真脸
            "R.Face",
            'DOB',
            'MinPrice',
            'MaxPrice',
            # 2022.2.11 增加球员位置，因为守门员的属性更多一些
            'Position',
            # pace 3
            'PACE',
            'Acceleration',
            'Sprint Speed',
            # shooting 7
            'SHOOTING',
            'Positioning',
            'Finishing',
            'Shot Power',
            'Long Shots',
            'Volleys',
            'Penalties',
            # PASSING 7
            'PASSING',
            'Vision',
            'Crossing',
            'FK. Accuracy',
            'Short Passing',
            'Long Passing',
            'Curve',
            # DRIBBLING 7
            'DRIBBLING',
            'Agility',
            'Balance',
            'Reactions',
            'Ball Control',
            'Dribbling',
            'Composure',
            # DEFENDING 6
            'DEFENDING',
            'Interceptions',
            'Heading Accuracy',
            'Def Awareness',
            'Standing Tackle',
            'Sliding Tackle',
            # PHYSICALITY 5
            'PHYSICALITY',
            'Jumping',
            'Stamina',
            'Strength',
            'Aggression',
            # 门将相关
            # 2
            'DIVING',
            'Diving',
            # 2
            'HANDLING',
            'Handling',
            # 2
            'KICKING',
            'Kicking',
            # 2
            'REFLEXES',
            'Reflexes',
            # 3
            'SPEED',
            'GK-Acceleration',
            'GK-Sprint Speed',
            # 2
            'POSITIONING',
            'GK-Positioning'
            ]
my_headers = [
    "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36",
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:30.0) Gecko/20100101 Firefox/30.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/537.75.14",
    "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Win64; x64; Trident/6.0)",
    'Mozilla/5.0 (Windows; U; Windows NT 5.1; it; rv:1.8.1.11) Gecko/20071127 Firefox/2.0.0.11',
    'Opera/9.25 (Windows NT 5.1; U; en)',
    'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)',
    'Mozilla/5.0 (compatible; Konqueror/3.5; Linux) KHTML/3.5.5 (like Gecko) (Kubuntu)',
    'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.8.0.12) Gecko/20070731 Ubuntu/dapper-security Firefox/1.5.0.12',
    'Lynx/2.8.5rel.1 libwww-FM/2.14 SSL-MM/1.4.1 GNUTLS/1.2.9',
    "Mozilla/5.0 (X11; Linux i686) AppleWebKit/535.7 (KHTML, like Gecko) Ubuntu/11.04 Chromium/16.0.912.77 Chrome/16.0.912.77 Safari/535.7",
    "Mozilla/5.0 (X11; Ubuntu; Linux i686; rv:10.0) Gecko/20100101 Firefox/10.0 "]

# 获取response 抓取页面
async def fetch_main(url: str):
    async with aiohttp.ClientSession() as session:
        # 设置超时时间
        timeout = aiohttp.ClientTimeout(total=120)

        headers = {'User-Agent': random.choice(my_headers)}
        try:
            async with session.get(url, timeout=timeout, headers=headers, proxy=proxy) as resp:
                if resp.status != 200:
                    print(url+"抓取失败")
                    print(resp.status)
                    return resp.status
                # print(url + "抓取成功")
                await asyncio.sleep(3)
                return await resp.text()
        except asyncio.exceptions.TimeoutError:
            url_fail.append(url)
            print(url+"请求超时")


# 解析第一页，主要目的是获取到页码数
def parser_list_page(main_html, first=False):
    selector = lxml.html.fromstring(main_html)
    if first:
        pages = selector.xpath("//ul[@class='pagination pg-blue justify-content-end']/li/a/text()")
        global maxPage
        if len(pages) == 0:
            maxPage = 1
        else:
            maxPage = pages[-4]

    # 球员名称列表
    players_name = selector.xpath("//tbody/tr/td/div[@class='d-inline pt-2 pl-3']/div/a/text()")
    players_pos = selector.xpath("//tbody/tr/td[3]/div[1]/text()")
    # 球员对应链接列表
    players_url = selector.xpath("//tbody/tr/@data-url")
    return players_name, players_url, players_pos


# 获取response 抓取价格页面
async def fetch_price(ID: str, session):
        # 设置超时时间
        url = "https://www.futbin.com/22/playerPrices?player=" + ID
        timeout = aiohttp.ClientTimeout(total=30)
        headers = {'User-Agent': random.choice(my_headers)}
        try:
            async with session.get(url, timeout=timeout, headers=headers, proxy=proxy) as resp:
                    if resp.status != 200:
                        raise asyncio.exceptions.TimeoutError
                    else:
                        await asyncio.sleep(1)
                        return await resp.text()
        except asyncio.exceptions.TimeoutError:
            return await fetch_price(ID, session)

# 解析价格界面获得最大值和最小值
def get_min_max_price(price_str):
    # results = re.search(r'"ps":.*?"MinPrice":"(.*?)".*?"MaxPrice":"(.*?)"', price_str)
    return 0,0


# 解析球员详情页面
async def parser_player_page(player_html, name, pos, session):
    # print(pos)
    # print("开始解析")
    player_info = dict()
    player_info["ShortName"] = name
    player_info["Intl. Rep"] = "0"
    # 2.14
    player_info["Position"] = pos
    selector = lxml.html.fromstring(player_html)
    player_info["NameOnCard"] = selector.xpath('//*[@id="Player-card"]/div[3]/text()')[0]
    print(player_info["NameOnCard"])
    # 左侧数据
    # 2022.2.10有个问题 player_html有可能这个table会缺失ID
    # 好像是user-agent的问题 删除了一些老旧的好了
    info_content = selector.xpath("//table[@class='table  table-info']/tr")
    for element in info_content:
        # 暂时没加解码
        element_str = lxml.html.tostring(element).decode("UTF-8")
        key_result = re.search(r'<th>((.|\n)*?)<', element_str)
        value_result = re.search(r'<a .*>((.|\n)*?)</a>', element_str)
        if value_result != None:
            value = value_result.group(1).strip()
        else:
            value_result = re.search(r'<td class="table-row-text">((.|\n)*?)<', element_str)
            value = value_result.group(1).strip()
        if key_result == None:
            continue
        else:
            key = key_result.group(1).strip()
            if key != '':
                if key == 'Age':
                    DOB_title = element.xpath("//a[@data-toggle='tooltip']/@title")
                    result = str.split(DOB_title[0], ' ')
                    value = result[2]
                    key = 'DOB'
                elif key == "Name" or key == "Nation":
                    value = unescape(value)
                elif key == "R.Face":
                    result = element.xpath('.//td/i/@class')
                    if result[0] == "icon-checkmark text-success":
                        value = "True"
                    elif result[0] == "icon-cross text-danger":
                        value = "False"
                player_info[key] = value

    # 总分 2.14
    score = selector.xpath('//*[@id="Player-card"]/div[2]/text()')
    player_info["Score"] = score[0]
    # print(f"Score-{player_info['Score']}")
    # 价格
    # 读取不了ID 重新找个地方读取
    if "ID" not in player_info.keys():
        ID_str = selector.xpath("//div[@id='page-info']/@data-baseid")
        #print(ID_str[0])
        player_info["ID"] = ID_str[0]

    price_str = await fetch_price(player_info["ID"], session)

    min_price, max_price = get_min_max_price(price_str)
    player_info["MinPrice"] = min_price
    player_info["MaxPrice"] = max_price
    # 右侧数据
    player_details = selector.xpath("//div[@class='container p-xs-0']/div[@id='player_stats_json']/text()")
    player_details = str(player_details[0])
    player_detail_json = json.loads(player_details)
    # print(player_detail_json)
    # PACE 3
    player_info["PACE"] = player_detail_json["ppace"]
    player_info["Acceleration"] = player_detail_json["acceleration"]
    player_info["Sprint Speed"] = player_detail_json["sprintspeed"]
    # SHOOTING 7
    player_info["SHOOTING"] = player_detail_json["pshooting"]
    player_info["Positioning"] = player_detail_json["positioning"]
    player_info["Finishing"] = player_detail_json["finishing"]
    player_info["Shot Power"] = player_detail_json["shotpower"]
    player_info["Long Shots"] = player_detail_json["longshotsaccuracy"]
    player_info["Volleys"] = player_detail_json["volleys"]
    player_info["Penalties"] = player_detail_json["penalties"]
    # PASSING 7
    player_info["PASSING"] = player_detail_json["ppassing"]
    player_info["Vision"] = player_detail_json["vision"]
    player_info["Crossing"] = player_detail_json["crossing"]
    player_info["FK. Accuracy"] = player_detail_json["freekickaccuracy"]
    player_info["Short Passing"] = player_detail_json["shortpassing"]
    player_info["Long Passing"] = player_detail_json["longpassing"]
    player_info["Curve"] = player_detail_json["curve"]
    # DRIBBLING 7
    player_info["DRIBBLING"] = player_detail_json["pdribbling"]
    player_info["Agility"] = player_detail_json["agility"]
    player_info["Balance"] = player_detail_json["balance"]
    player_info["Reactions"] = player_detail_json["reactions"]
    player_info["Ball Control"] = player_detail_json["ballcontrol"]
    player_info["Dribbling"] = player_detail_json["dribbling"]
    player_info["Composure"] = player_detail_json["composure"]
    # DEFENDING 6
    player_info["DEFENDING"] = player_detail_json["pdefending"]
    player_info["Interceptions"] = player_detail_json["interceptions"]
    player_info["Heading Accuracy"] = player_detail_json["headingaccuracy"]
    player_info["Def Awareness"] = player_detail_json["marking"]
    player_info["Standing Tackle"] = player_detail_json["standingtackle"]
    player_info["Sliding Tackle"] = player_detail_json["slidingtackle"]
    # PHYSICALITY 5
    player_info["PHYSICALITY"] = player_detail_json["pphysical"]
    player_info["Jumping"] = player_detail_json["jumping"]
    player_info["Stamina"] = player_detail_json["stamina"]
    player_info["Strength"] = player_detail_json["strength"]
    player_info["Aggression"] = player_detail_json["aggression"]
    # 判断是否是守门员
    if pos != "GK":
        player_info["DIVING"] = 0
        player_info["Diving"] = 0
        player_info["HANDLING"] = 0
        player_info["Handling"] = 0
        player_info["KICKING"] = 0
        player_info["Kicking"] = 0
        player_info["REFLEXES"] = 0
        player_info["Reflexes"] = 0
        player_info["SPEED"] = 0
        player_info["GK-Acceleration"] = 0
        player_info["GK-Sprint Speed"] = 0
        player_info["POSITIONING"] = 0
        player_info["GK-Positioning"] = 0
    else:
        gk_data = selector.xpath("//div[@id='player_stats_json'][1]/text()")
        gk_json = json.loads(str(gk_data[0]))
        gk_dict = gk_json[0]
        player_info["DIVING"] = gk_dict["gkdiving"][0]["value"]
        player_info["Diving"] = gk_dict["gkdiving"][1]["value"]
        player_info["HANDLING"] = gk_dict["gkhandling"][0]["value"]
        player_info["Handling"] = gk_dict["gkhandling"][1]["value"]
        player_info["KICKING"] = gk_dict["gkkicking"][0]["value"]
        player_info["Kicking"] = gk_dict["gkkicking"][1]["value"]
        player_info["REFLEXES"] = gk_dict["gkreflexes"][0]["value"]
        player_info["Reflexes"] = gk_dict["gkreflexes"][1]["value"]
        player_info["SPEED"] = gk_dict["speed"][0]["value"]
        player_info["GK-Acceleration"] = gk_dict["speed"][1]["value"]
        player_info["GK-Sprint Speed"] = gk_dict["speed"][2]["value"]
        player_info["POSITIONING"] = gk_dict["gkpositioning"][0]["value"]
        player_info["GK-Positioning"] = gk_dict["gkpositioning"][1]["value"]

    print(f"球员{name},解析成功")
    return player_info


#
async def fetch_player(url: str, session):
        # 设置超时时间
        timeout = aiohttp.ClientTimeout(total=120)
        headers = {'User-Agent': random.choice(my_headers)}
        try:
            async with session.get(url, timeout=timeout, headers=headers, proxy=proxy) as resp:
                if resp.status != 200:
                    print(url+"抓取失败")
                    print(resp.status)
                    return resp.status
                # print(url + "抓取成功")
                await asyncio.sleep(3)
                return await resp.text()
        except asyncio.exceptions.TimeoutError:
            url_fail.append(url)
            print(url+"请求超时")


# 抓取和解析放在一个函数里
async def fetch_and_parser(url, session, name, pos, semaphore):
    async with semaphore:
        # await asyncio.sleep(30)
        html = await fetch_player(url, session)
        if html != 200:
            player_info = await parser_player_page(html, name, pos, session)
            return player_info
        else:
            print(f"{name} 没有保存成功")


async def get_each_player(name_list, url_list, pos_list, semaphore):
    print(pos_list)
    lenth = len(name_list)
    async with aiohttp.ClientSession() as session:
        result_list = await asyncio.gather(*[fetch_and_parser("https://www.futbin.com" + url_list[index], session, name_list[index], pos_list[index], semaphore) for index in range(0, lenth)])
        player_list.extend(result_list)


# 保存为csv
def save_datas_to_csv(page, first=False):
    with open("footballers.csv", "a+", encoding='UTF-8') as csvfile:
        position = csvfile.tell()
        write = csv.writer(csvfile, delimiter=',')
        if position == 0:
            write.writerow(csv_head)
        for player in player_list:
            write.writerow([player[key] for key in csv_head])
        player_list.clear()
        print(f"第{page}页，解析完成")


# csv转换为excel
def csv_to_excel():
    read_file = pandas.read_csv('footballers.csv')
    read_file.to_excel('footballers.xlsx', header=True)


if __name__ == '__main__':
    # 要报错 Event loop is closed 用这个试试
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    first_url = input("请输入第一个页面（页面链接参考见文档，输入toExcel会把已有csv转存为xlsx）：")
    if first_url == "toExcel":
        csv_to_excel()
    else:
        while(True):
            type_str = input("是否使用代理 Y/N:")
            if type_str == "Y" or type_str == "y":
                # proxy = input("请输入代理地址（格式参考http://127.0.0.1:7890）：")
                proxy = "http://127.0.0.1:7890"
                break
            elif type_str == "N" or type_str == "n":
                break
        while (True):
            try:
                semaphore_1 = asyncio.Semaphore(5)
                main_html = asyncio.run(fetch_main(first_url))
                havePage = re.search(r'page=(.*?)&', first_url)
                # 设置第一页和地址链接模板
                if havePage == None:
                    firstPage = 1
                    url_template = first_url + "?page={0}"
                else:
                    firstPage = int(havePage.group(1))
                    url_template = first_url.replace(havePage.group(0), 'page={0}&')
                # print(firstPage)
                # print(url_template)
                # 获取最大页数和第一页的球员列表
                players_name, players_url, players_pos = parser_list_page(main_html, True)
                asyncio.run(get_each_player(players_name, players_url, players_pos, semaphore_1))
                save_datas_to_csv(firstPage)
                first_csv = False
                maxPage = int(maxPage)
                if firstPage != maxPage:
                    for page in range(firstPage + 1, maxPage + 1):
                        semaphore = asyncio.Semaphore(5)
                        page_str = str(page)
                        need_fetch_url = url_template.format(page_str)
                        main_html = asyncio.run(fetch_main(need_fetch_url))
                        players_name, players_url, players_pos = parser_list_page(main_html)
                        asyncio.run(get_each_player(players_name, players_url, players_pos, semaphore))
                        save_datas_to_csv(page)
                        # time.sleep(30)
                    break
                else:
                    break
            except aiohttp.client_exceptions.ClientConnectorError:
                print(f"\033[7;31m第{page}解析失败，重新开始解析\033[0m")
                first_url = url_template.format(str(page))
            except BaseException as e:
                print(f"\033[7;31m在第{page}爬取失败\033[0m")
                print(e)
                break

        input("=====================防止不闪退=================")

