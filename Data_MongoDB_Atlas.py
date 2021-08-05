"""
username: dataHandler
password: mydrives123

client = pymongo.MongoClient("mongodb+srv://<username>:<password>@dpa-db.rgdit.azure.mongodb.net/clusterName?retryWrites=true&w=majority")
db = client.test
"""
import datetime
from pymongo import MongoClient

client = MongoClient("mongodb+srv://dataHandler:mydrives123@dpa-db.rgdit.azure.mongodb.net/DPA-DB"
                     "?retryWrites=true&w=majority")
db = client.get_database("mydrives_data")
manage = db.DataManagement  # use collection
count = manage.count_documents({})
print(count)


class DrivingData:
    def __init__(self, username, school):
        self.username = username
        self.school = school

    def new_user(self):
        join_date = datetime.datetime.now()
        data = {
            "userId": self.username,
            "member_since": join_date,
            "driving_school": self.school,
            "records": [
                {
                    "driving_starts_timestamp": 0,
                    "videos": [],
                    "driving_ends_timestamp": 0
                }
            ]
        }
        manage.insert_one(data)
    '''
    查询可以使用投影，类似于SQL的select语句表示指定查询结果只要保留那些内容，例如{"records": 1, "_id": 0}就是投影
    列出的键值对且值为1表示选择保留，值为0表示指定不显示，未列出的键值对默认不显示
    但如果没有设置投影，则默认显示查询文档的所有内容
    '''
    def find_data(self, username):
        data = manage.find_one({"userId": username}, {"records": 1, "_id": 0})
        print(data)

    '''
    注意更新操作符，在pymongo中，操作符要加引号，例如"$push"
    "$push"表示数组插入操纵，首先它的操作对象必须是数组(列表)，其次它不是$set设置，它的作用是向数组推入新增内容
    $push还可以和$each连用，如果要一次向数组推入多个项，可以使用$each，videoInfo_update有使用
    '''
    def update_records(self, username, timestamp_start, timestamp_end, *video_urls):
        data = generate_records(timestamp_start, timestamp_end, *video_urls)
        manage.update_one({"userId": username}, {
            "$push": {
                "records": data
            }
        })

    '''
    注意$push和$each连用的用法
    本函数使用到了$[t]这样的$[<identifier>]表示符操作，在多重嵌套的结构中，$[<identifier>]用于表示一个数组结构的占位符
    如 "records.$[t].videos" 这样的结构，$[t]就表示records值的数组，
    t占位表示数组中任意一个元素(本例中元素是包裹3个键值对的字典)，$[t].videos表示查找任意一个t字典的videos键，然后推入值
    t必须要有具体的表达式明确其究竟是哪个字典元素，因此需要array_filters数组选择器，用其表达式，如本例，找出这个t是
    符合driving_starts_timestamp": timestamp的这个字典，这样"records.$[t].videos"就准确找到了要update的具体字典元素
    在pymongo中array_filters是**kw关键字参数，有的语言会写成map键值对{"arrayFilters":[{xx:yy}]}
    upsert表示如果在需要更新记录的地方不存在原记录，是否要将新纪录插入
    '''
    def videoInfo_update(self, username, timestamp, *video_update):
        manage.update_one({"userId": username},
                          {
                              "$push": {"records.$[t].videos": {
                                  "$each": video_update
                              },
                              }
                          },
                          array_filters=[{"t.driving_starts_timestamp": timestamp}],
                          upsert=False
                          )


# functions outside class
def generate_records(timestamp_start, timestamp_end, *video_urls):
    start = timestamp_start
    end = timestamp_end
    videos = [video_url for video_url in video_urls]

    record_data = {
        "driving_starts_timestamp": start,  # 1627809630, '2021-08-01 11:20:30'
        "videos": videos,
        "driving_ends_timestamp": end  # 1627813227, '2021-08-01 12:20:27'
    }
    return record_data


user_jack = DrivingData("Jack", "Atlantic_driver")
user_rose = DrivingData("Rose", "Titanic_driver")
# user_jack.new_user()
# user_rose.new_user()
# user_jack.update_records("Jack", 1627809630, 1627813227, "url_1", "url_2", "url_3")
# user_rose.update_records("Rose", 1627896311, 1627899955, "url_4", "url_5", "url_6")
# user_jack.update_records("Jack", 1627974015, 1627978116, "url_7", "url_8", "url_9")
# user_rose.videoInfo_update("Rose", 1627896311, "url_x", "url_y", "url_z")
user_jack.find_data("Jack")
user_rose.find_data("Rose")
