from ebaysdk.finding import Connection as Finding
from ebaysdk.exception import ConnectionError
from ebaysdk.trading import Connection as Trading
import  datetime
import  pymysql
from src.services.base_service import BaseService
from configs.config import Config
from concurrent.futures import ThreadPoolExecutor, as_completed



class EbayFindListByKeywords(BaseService):
    def __init__(self):
        super().__init__()
        self.config = Config().get_config('ebay.yaml')

    def getData(self,keywords):
        try:
            api = Finding(config_file=self.config)
            response = api.execute(
                'findItemsByKeywords',
                {
                    'keywords':keywords,
                    # 'itemFilter':{
                    #     'EndTimeFrom': startTimeFrom,
                    #     'EndTimeTo': startTimeTo,
                    # },
                    # 'DetailLevel':'ItemReturnDescription',
                    'detailLevel': 'ReturnAll',
                    # 'requesterCredentials': {'eBayAuthToken': token},
                    'paginationInput': {
                        'entriesPerPage': 100,
                        'pageNumber': 1,
                    }
                }
            )
            result = response.dict()
            # print(result)
            resList = []
            if result['paginationOutput']['totalPages'] == '0':
                pass
            elif result['paginationOutput']['totalPages'] == '1':
                itemList = result['searchResult']['item']
                for goodsItem in itemList:
                    goodsInfo = self.getItemInfo(goodsItem)
                    resList.append(goodsInfo)
            else:
                totalPages = int(result['paginationOutput']['totalPages']) if int(result['paginationOutput']['totalPages'])<=100 else 100
                for i in range(totalPages):
                    if i==0:
                        itemList = result['searchResult']['item']
                        for goodsItem in itemList:
                            goodsInfo = self.getItemInfo(goodsItem)
                            resList.append(goodsInfo)
                    else:
                        try:
                            newResponse = api.execute(
                                'findItemsByKeywords',
                                {
                                    'keywords':keywords,
                                    # 'itemFilter':{
                                    #     'EndTimeFrom': startTimeFrom,
                                    #     'EndTimeTo': startTimeTo,
                                    # },
                                    # 'DetailLevel':'ItemReturnDescription',
                                    'detailLevel': 'ReturnAll',
                                    # 'requesterCredentials': {'eBayAuthToken': token},
                                    'paginationInput': {
                                        'entriesPerPage': 100,
                                        'pageNumber': i + 1,
                                    }
                                }
                            )
                            newList = newResponse.dict()
                            # print(len(newList))
                            if newList['ack'] == 'Success' or newList['Ack'] == 'Success':
                                if newList['searchResult']['_count'] == '1':
                                    item = self.getItemInfo(newList['searchResult']['item'])
                                    resList.append(item)
                                else:
                                    for j in newList['searchResult']['item']:
                                        itemArr = self.getItemInfo(j)
                                        resList.append(itemArr)
                        except ConnectionError as e:
                            self.logger.error(e)

            return resList
        except ConnectionError as e:
            self.logger.error(e)
            return []

    def getItemInfo(self,goodsItem):
        utc_format = '%Y-%m-%dT%H:%M:%S.%fZ'
        local_format = '%Y-%m-%d %H:%M:%S'
        StartTime = datetime.datetime.strptime(goodsItem['listingInfo']['startTime'], utc_format)
        EndTime = datetime.datetime.strptime(goodsItem['listingInfo']['endTime'], utc_format)
        ItemID = goodsItem['itemId']

        StartTime = StartTime.strftime(local_format)
        EndTime = EndTime.strftime(local_format)
        Title = pymysql.escape_string(goodsItem['title'])

        ViewItemURL = goodsItem['viewItemURL']
        CategoryID = goodsItem['primaryCategory']['categoryId']
        if 'categoryName' in goodsItem['primaryCategory']:
            CategoryName = pymysql.escape_string(goodsItem['primaryCategory']['categoryName'])
        else:
            CategoryName = ''

        GalleryURL = goodsItem['galleryURL'] if 'galleryURL' in goodsItem else ''
        CreateTime = datetime.datetime.now().strftime(local_format)

        CurrencyCode = goodsItem['sellingStatus']['currentPrice']['_currencyId']
        Price = goodsItem['sellingStatus']['currentPrice']['value']

        goodsInfo = (ItemID, StartTime, EndTime, Title, ViewItemURL, CategoryID, CategoryName,
                     CurrencyCode, Price, GalleryURL, CreateTime)
        return  goodsInfo

    def saveToMySql(self,goodsInfo):

        sql = (
                "insert into ebay_data(ItemID, StartTime, EndTime, Title, ViewItemURL, PayPalEmailAddress, CategoryID, CategoryName," +
                "CurrencyCode, Price, Quantity, QuantitySold, HitCount," +
                "SKU, GalleryURL, PictureURL, StoreName, CreateTime) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        )
        self.cur.executemany(sql, goodsInfo)


        # 提交数据,必须提交，不然数据不会保存
        self.con.commit()
        self.cur.close()
        self.con.close()

    def run(self):
        # BeginTime = time.time()
        try:
            keywordsArr = [
                'headPhones',
                'dress',
                'Necklace',
                'Ring',
                'shoes',
                'Boots',
                'balloon',
                'Toys',
                'book',
                'Umbrella',
                'coat',
                'T-shirt',
                'Christmas',
                'beats',
                'iPod',
                'pm3',
                'guitars',
                'basses',
            ]
            with ThreadPoolExecutor(10) as pool:
                future = {pool.submit(self.getData, keywords): keywords for keywords in keywordsArr}
                for fu in as_completed(future):
                    try:
                        data = fu.result()
                        self.logger.info(len(data))
                        # for row in data:
                        #     self.save_data(row)
                    except Exception as e:
                        self.logger.error(e)
        except Exception as e:
            self.logger.error(e)
        finally:
            self.close()
        # print('程序耗时{:.2f}'.format(time.time() - BeginTime))  # 计算程序总耗时
        # exit(0)


#执行程序
if __name__ == "__main__":
    worker = EbayFindListByKeywords()
    worker.run()

