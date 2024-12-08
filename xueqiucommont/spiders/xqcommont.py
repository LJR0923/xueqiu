import scrapy
from typing import Iterable
from urllib.parse import urlencode
import scrapy
from scrapy import Request
from scrapy import FormRequest
import subprocess
from functools import partial
subprocess.Popen = partial(subprocess.Popen, encoding='utf-8')
import time
from datetime import datetime, timedelta
import requests
Oneday_Switch = False  # 是否只抓1天内的 开关
Recommond = False # 抓最热
# web端ck
ck1 = {
            "cookiesu": "561729585809901",
            "device_id": "a595a3e318a81b92676a202f9dd70d4a",
            "remember": "1",
            "xq_a_token": "8bfd99c747c55819552bd53ed230ab9c17cba7ab",
            "xqat": "8bfd99c747c55819552bd53ed230ab9c17cba7ab",
            "xq_id_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOjQxNTI3MTgzMTgsImlzcyI6InVjIiwiZXhwIjoxNzMyMjg0MDYyLCJjdG0iOjE3Mjk2OTIwNjIzODcsImNpZCI6ImQ5ZDBuNEFadXAifQ.MaBzZwC1X6HRf_NayuvHKNCRSuiUp48YYCL6cw4LBCComGwyDSz5rOGvNyX5g4fvCqxMmHqdKFkJDxrCnJDaY13yNerjlpe4QRLYePH868J6Ac53Ht541zya1lerk9n4A_ljtW0DGVNR1cfP5W2AjsjiY6pw5ddMItVxlICXZ_PrCzXkrm-14FuKdSFjZicjIDVDM8IeCsLxYwrkK6huVBQMpLcBHdrVFoedf9IuEos28aYKLVuIo6oUuD1Kkeuorvw9fFbhsEH0OpuhtCELT2FRaHdmTLnNFmUv0fjfWgJ7EqzUslh0_Dc1fUZQNtTGkH7UkEf4SL2LEQ3Mrzu8xQ",
            "xq_r_token": "c55b57aa878a2d06a68d8f58bf02a6d8d72f11ff",
            "xq_is_login": "1",
            "u": "4152718318",
            "ssxmod_itna": "Qq+OAKDK4UgDCDnDl4Yq0pn6tkaitD9larW1RlQo1nx05rdGzDAxn40iDt=O5Zm0b+33tC0p+xFte3xjYrCbjoj3QYBROi4TDCPGnDB9DtIIDYY1Dt4DTD34DYDir5DLDmeD+UsKDd06TN/GT=D3qDwDB=DmqG23etDm4DfDDdBKYhIIdxD0dxi5S5KDYPBRMLPdjrDAkGlYGDBD0tDIqGXtB9eGqDB0aye/dXHwpWHWd2exBQD7qRM4L2dId0ZtwWd+0p3AYA3WgYqYBhN+8GiAGGnH5ANeEq4+GhetY4Mg+dKS7+eDD34jGB=hYYD=",
            "ssxmod_itna2": "Qq+OAKDK4UgDCDnDl4Yq0pn6tkaitD9larW1RlQo1DnFkqxDsI5sDLebe1VZl1YnDOD8ER=4gmWmDU5yKef7mweomC0w+NKCwBxzWdK+hyYp5mAA=u5UPOAcfabDZGQzF1NA+jx1oSEYUw3A3TQqW43cYSNm+iiEY3miSxvAIfLYfCGY7ftTGbLUt3e6z3rQq64=ML+031I1ifRYm6Uw9eNXb3YVl+uOfTXy7QnK1qtI3uY=Zn3X36XKzefh3gI39TfUUR88iSAqI9dnm3oChWIHr4d6fDcdX9=j5Gx8StyOBruhbSVxZLSno0a5sYiMGi4+UyYpC6Y8me9=GNiqihIOivkWvYAKKb5OhqfhGcrfUir8WKBZIhmNABf=UAEpeAhE4OeABEue=Yb27fjOpmYBEZmusZeahmd2AT+I3tEj2XO7Pn02/ieazm4Wmwmf38iYFhiBK+jEk0G10GBiuLWdRo56fKC7PrYfcaow0DnBofOeR0bO0jTWvqQa4Qjervee42imNnp4swqev4n=BWQIemCGpvcmRCDyW7I7dmleT2mvGT/jPzxrAbBB0EUAwNctyQ9DMpT3PzneWrbAOePC9FK=zQOlEkvIb1sM/DYyiG5YoxHLM7PDr=k28jVbOQQgZ1umiAhd00cFT2FDG2bTe/Gq+Y2pBx3ubtC9bDqfnq0KxfXFB=n2/1qZHblqFebD8A+nP1byqYdVu49tNGkm4oYAaoaGqlY1bPg=O790hQG5Oi1b/aKm7DIeNKNQGPB5F5QDYeap25mD9q8osuYtl395qDDLxD+EKPG4Czvixn5DhDD="
        }
# 手机端ck
ck2 = {
            "xq_a_token": "d0c1dc492fa232c79e4f20da33ee843abbe14cff",
            "xq_id_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJ1aWQiOjMxNjU2NTE5NTUsImlzcyI6InVjIiwiZXhwIjoxNzMyMTY5MDA2LCJjdG0iOjE3Mjk1NzcwMTE3NzIsImNpZCI6Ikp0WGJhTW43ZVAifQ.RosQ0I-MjQsZkTqCq00LT1dfIEKBHLyhL67DQNeVknurRcJAtlSf0UZ1mdb8vIJQz2kK2Mj8fnPsF36O3hjQkPtr-p-4IDqmlKRZfRWBC10R_ag5LQGtkrcAsxsFi-9NRb2nFmIK7WkQBI4VladC-yM7LHc3zrUr7z7QO1GRsI3_DWQ314C061oAZ6Q1lEXS_5AqfnCS_EovwTudDjKwMfd71YWvJam5eVYeRRq-o9MB5Pe671IoBplQThfva2k3LeUk2dH7jSgmKYBwVEQcg6ES8x3Tp56zr1tkPuOwwUkWDNkTJBMsXKZ3QStfbY8sMa6_qSa05c77OVVQuPrM7Q",
            "u": "3165651955",
            "session_id": "",
            "xid": "0"
        }
headers = {
            "accept": "application/json, text/plain, */*",
            "accept-language": "zh-CN,zh;q=0.9",
            "cache-control": "no-cache",
            "origin": "https://xueqiu.com",
            "pragma": "no-cache",
            "priority": "u=1, i",
            "referer": "nullhttps://xueqiu.com/ai/stock/summary?id=6536749495510&symbol=SZ002583&share_type=weixin&data_type=link&data_model=sd&fix_uid=3937332793",
            "sec-ch-ua": "\"Google Chrome\";v=\"129\", \"Not=A?Brand\";v=\"8\", \"Chromium\";v=\"129\"",
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-site",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
        }
def has_time_exceeded_one_day(timestamp):
    timestamp = timestamp/1000
    # 当前时间的时间戳
    current_timestamp = datetime.timestamp(datetime.now())
    # 检查是否超过一天（86400秒）
    return (current_timestamp - timestamp) > 86400
def generate_timestamps():
    # 获取当前时间的时间戳（秒）
    now_seconds = time.time()

    # 计算当前时间的时间戳（毫秒）
    current_timestamp_ms = int(now_seconds * 1000)

    # 计算1小时前的时间戳（秒）
    one_hour_ago_seconds = now_seconds - 3600

    # 计算1小时前的时间戳（毫秒）

    one_hour_ago_timestamp_ms = int(one_hour_ago_seconds * 1000)

    # 返回当前时间戳和1小时前的时间戳（毫秒）
    return str(current_timestamp_ms)+'.'+str(one_hour_ago_timestamp_ms)
def adjust_timestamp(timestamp):
    # 将时间戳转换为字符串，以便检测长度
    timestamp_str = str(timestamp)

    # 检测时间戳长度
    if len(timestamp_str) == 16:
        # 如果长度为16，则除以1000
        return timestamp / 1000
    else:
        # 如果长度不是16，则返回原始时间戳
        return timestamp
def parse_timestamp(created_at):
    # 尝试将字符串转换为 Unix 时间戳（秒）
    try:
        # 直接转换为秒级时间戳，然后转换为毫秒
        return float(created_at)
    except ValueError:
        # 如果转换失败，说明 created_at 是一个日期字符串
        # 定义日期字符串的格式
        date_format = "%Y年%m月%d日 %H:%M:%S"
        # 将字符串转换为 datetime 对象
        date_obj = datetime.strptime(created_at, date_format)
        # 将 datetime 对象转换为 Unix 时间戳（秒），然后转换为毫秒
        print(datetime.timestamp(date_obj) * 1000,"aaaaa啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊啊")
        return datetime.timestamp(date_obj) * 1000
mapp = {
    "CN":"sh_sz",

}

class XqcommontSpider(scrapy.Spider):
    name = "xqcommont"
    allowed_domains = ["stock.xueqiu.com",'xueqiu.com',"api.xueqiu.com"]
    # start_urls = ["https://xueqiu.com"]

    def start_requests(self):
        url = "https://stock.xueqiu.com/v5/stock/screener/quote/list.json"
        cookies = ck1
        header = headers
        for i in mapp:
            market = i
            type = mapp[i]
            if type == "us":
                end_page = 168
            else:
                end_page = 168
            for i in range(1, end_page):
                params = {
                    "page": i,
                    "size": "30",
                    "order": "desc",
                    "order_by": "percent",
                    "market": market,
                    "type": type
                }
                # 使用 urlencode 方法来编码查询参数
                encoded_params = urlencode(params)
                # 将编码后的参数附加到 URL 上
                full_url = f"{url}?{encoded_params}"
                print(full_url)
                req = Request(full_url, cookies=cookies, callback=self.parse, headers=header, meta={"Stock_type": type})
                print(1)
                yield req
                # break
            # break

    def parse(self,resp):
        # print("parse")
        Stock_type = resp.meta.get("Stock_type")
        reslist = resp.json()["data"]['list']
        for item in reslist:
            req = self.first_floors(item['symbol'],1,Stock_type)
            yield req
            # break

    def first_floors(self,symbol,page,Stock_type):
        # print("page",page)
        cookies = ck2
        shijianchuo = generate_timestamps()
        if Recommond:
            type = "Recommond"
            url = f"https://api.xueqiu.com/query/v1/symbol/recommend/status.json?symbol={symbol}&count=10&sort=recommend&page={page}&_t=1XIAOMI70a18e6c3ff9ccb289e91f90b4ede769.3165651955.{shijianchuo}&_s=f746a5"
        else:
            type = "basic"
            url = f"https://api.xueqiu.com/query/v1/symbol/search/status.json?filter_text=1&symbol={symbol}&hl=0&count=10&comment=0&source=all&sort=time&page={page}&type=11&_t=1XIAOMI70a18e6c3ff9ccb289e91f90b4ede769.3165651955.{shijianchuo}&_s=049afa"
        print("firstFloor",url)
             #  "https://api.xueqiu.com/query/v1/symbol/recommend/status.json?symbol=SZ301592&count=10&sort=recommend&page=1" 最热
        req = Request(url, cookies=cookies, callback=self.First_floor_Parse,meta={"Stock_type": Stock_type,"symbol":symbol,"page":page,"type":type})
        return req
    def First_floor_Parse(self,resp):
        shijianchuo = generate_timestamps()
        dic = {}
        # print(resp.text)
        Stock_type = resp.meta.get("Stock_type")
        symbol = resp.meta.get("symbol")
        page = resp.meta.get("page")
        type = resp.meta.get("type")
        if type == "basic":
            commontOne = resp.json()["list"]
        else:
            commontOne = resp.json()["data"][0]["list"]
        for item in commontOne:
            if item["description"].endswith('...'):
                url = "https://api.xueqiu.com/statuses/show.json"
                params = {
                    "id": item["id"],
                    "_h5": "1723458976249",
                    "_t": f"1XIAOMI70a18e6c3ff9ccb289e91f90b4ede769.3165651955.{shijianchuo}",
                    "_s": "e7b139"
                }
                response = requests.get(url, headers=headers, cookies=ck2, params=params,verify=False)
                item["description"] = response.json()["text"]
            dic["commont_floor"] = "first"  # 评论层级
            dic["Stock_type"] = Stock_type  # 股票类型 美股还是A股
            dic["data"] = item
            dic["data"]["symbol"] = symbol
            # time.sleep(3)
            if Oneday_Switch:
                print(has_time_exceeded_one_day(item["created_at"]),item["created_at"])
                if has_time_exceeded_one_day(item["created_at"]):
                    # print(item["description"])
                    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++===========================================================时间超过一天,跳过")
                    break
                else:
                    # print(item["description"])
                    req = self.Sceond_floor(item["id"],20,Stock_type,symbol)
                    yield req
                    yield dic
            else:
                req = self.Sceond_floor(item["id"], 20, Stock_type,symbol)
                yield req
                yield dic
            # break
        print("commontOne",len(commontOne))
        if Oneday_Switch:
            try:
                created_at = commontOne[-1].get("created_at",1719191983000)
                created_at = parse_timestamp(created_at)
                # created_at = adjust_timestamp(created_at)
                print(type(created_at),"created_at",created_at)
                # print(commontOne)
            except Exception as e:
                created_at = 1719191983000
            if has_time_exceeded_one_day(created_at):
                # print(commontOne[-1].get("description",1719191983000))
                print("时间一级超过一天")
            else:
                # print("s")
                req = self.first_floors(symbol, page+1, Stock_type)
                yield req
        elif len(commontOne) < 1:
            print("第一层没有了",commontOne)
        else:
            req = self.first_floors(symbol, page + 1, Stock_type)
            yield req
    def Sceond_floor(self,id,size,Stock_type,symbol):
        cookies = ck2
        shijianchuo = generate_timestamps()
        url = f"https://api.xueqiu.com/statuses/v3/comments.json?size={size}&max_id=0&id={id}&child_type=1&type=4&_t=1XIAOMI70a18e6c3ff9ccb289e91f90b4ede769.3165651955.{shijianchuo}&_s=049afa"
        print("Sceond_floor",url)
        req = Request(url,cookies=cookies,callback=self.Sceond_floor_parse,meta={"Stock_type": Stock_type, "id": id, "size": size,"symbol":symbol})
        return req

    def Sceond_floor_parse(self,resp):
        # print(resp.text)
        dic = {}
        Stock_type = resp.meta.get("Stock_type")
        symbol = resp.meta.get("symbol")
        id = resp.meta.get("id")
        size = resp.meta.get("size")
        commontTwo = resp.json()["comments"]
        for item in commontTwo:
            dic["commont_floor"] = "second"  # 评论层级
            dic["Stock_type"] = Stock_type  # 股票类型 美股还是A股
            dic["data"] = item
            dic["data"]["symbol"] = symbol
            # print(item["description"])
            if item["reply_count"] != 0:
                req = self.Third_floor(id,item["id"],20,Stock_type)
                yield req
            yield dic
        if resp.json()["next_max_id"] == -1:
            print("")
        else:
            req = self.Sceond_floor(id, size+20, Stock_type,symbol)
            yield req

    def Third_floor(self, id,comment_id, size, Stock_type):
        shijianchuo = generate_timestamps()
        url = f"https://api.xueqiu.com/statuses/v3/comments.json?size={size}&max_id=0&id={id}&child_type=1&comment_id={comment_id}&type=4&_t=1XIAOMI70a18e6c3ff9ccb289e91f90b4ede769.3165651955.{shijianchuo}&_s=049afa"
        print("Third_floor", url)
        req = Request(url,callback=self.Third_floor_parse,meta={"Stock_type": Stock_type, "id": id,"comment_id":comment_id, "size": size})
        return req

    def Third_floor_parse(self, resp):
        # print(resp.text)
        dic = {}
        comment_id = resp.meta.get("comment_id")
        Stock_type = resp.meta.get("Stock_type")
        id = resp.meta.get("id")
        size = resp.meta.get("size")
        commontTwo = resp.json()["comments"]
        for item in commontTwo:
            dic["commont_floor"] = "third"  # 评论层级
            dic["Stock_type"] = Stock_type  # 股票类型 美股还是A股
            dic["data"] = item
            yield dic
        if resp.json()["next_max_id"] == -1:
            print("")
        else:
            req = self.Third_floor(id,comment_id, size + 20, Stock_type)
            yield req