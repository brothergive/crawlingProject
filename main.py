# import
import datetime
from collections import Counter
import psycopg2 as pg2
import requests
from bs4 import BeautifulSoup
from konlpy.tag import Mecab

# DB connection
conn = pg2.connect(dbname='postgres', user='postgres', password='12300', host='127.0.0.1', port='5432')
curs = conn.cursor()

# set
# set.date
startDate = datetime.date(2021, 5, 1)   # 시작 날짜
endDate = datetime.date(2021, 8, 31)    # 종료 날짜
day_delta = datetime.timedelta(days=1)  # 날짜 간격
# set.requests.header
header = {"User-Agent": "os_windows chrome pc version_90_0_4430_225"}
# set.MecabDictionary
mecab = Mecab(dicpath='C:\mecab\mecab-ko-dic' )
# set.attribute
attr = {"class": "link_txt", "data-tiara-layer": "", "aria-hidden": ""}

# data
# data.url
rankUrl = "https://news.daum.net/ranking/popular/news?regDate="
newsUrl = "https://news.v.daum.net/v/"

print(startDate.strftime("%Y년%m월%d일 부터")+endDate.strftime("%Y년%m월%d일 까지의 랭킹뉴스 data 수집을 시작합니다."))
for i in range((endDate-startDate).days+1):     # 지정된 기간을 순회합니다.
    rankDate = startDate+i * day_delta  # 날짜 계산
    dailyNewsUrl = rankUrl + rankDate.strftime("%Y%m%d")    #날짜별 ranking news url을 만듭니다. 
    print(rankDate.strftime("%Y년%m월%d일의 랭킹뉴스 수집을 시작합니다.>>url: ") + dailyNewsUrl)
    startTime = datetime.datetime.now() # 시작 시간 채크
    response = requests.get(dailyNewsUrl, headers=header)   #날짜별 ranking news에 요청
    data = BeautifulSoup(response.text, "html.parser")
    cnt = 1 # ranking 표현
    for link in data.find_all("a", attrs=attr)[:50]:    # 날짜별 ranking news url 순회
        print("\nrank: "+str(cnt)+"  news 수집 시작!")
        cnt += 1
        articleUrl = link["href"]
        url = articleUrl[len(newsUrl)-5:]

        # ranknews table에 날짜별 랭킹 뉴스 url을 저장합니다.
        # 중복 방어
        curs.execute("SELECT COUNT(*) FROM ranknews WHERE rankdate = '{rankDate}' AND url = '{url}'".format(rankDate=rankDate, url=url))
        if curs.fetchone()[0] == 0:
            # ranknews table에 날짜별 랭킹 뉴스 url을 저장합니다.
            sqlString = "INSERT INTO ranknews(rankdate, url) VALUES ('{rankDate}',{url})".format(rankDate=rankDate, url=url)
            curs.execute(sqlString)
            conn.commit()

            sqlString = "SELECT COUNT(*) FROM newsinfo WHERE url='{url}'".format(url=url)
            curs.execute(sqlString)
            if curs.fetchone()[0] == 0: # news info 중복 저장 방어
                response = requests.get(articleUrl, headers=header) # news에 요청
                articleData = BeautifulSoup(response.text, "html.parser")
                articleTitle = articleData.find("meta", {"property" : "og:title"})["content"]   # 제목 수집
                articleAuthor = articleData.find("meta", {"property": "og:article:author"})["content"]  # 언론사 수집
                uploadDate = articleData.find("meta", {"property": "og:regDate"})["content"]    # 작성일 수집
                articleClassification = articleData.find(id="kakaoBody").string # 분류 수집
                if articleClassification is None:   # 분류 수집에 대한 exception처리
                    articleClassification = '기타'
                articleContent = articleData.find(class_="news_view").get_text()    # 내용 수집

                # DB 저장
                curs.execute("INSERT INTO newsinfo(title, classification, uploaddate, url, author) VALUES(%s, %s, %s, %s, %s)", (articleTitle, articleClassification, uploadDate[:4] + "-" + uploadDate[4:6] + "-" + uploadDate[6:8] + " " + uploadDate[8:10] + ":" + uploadDate[10:12] + ":" + uploadDate[12:], url, articleAuthor))
                conn.commit()

                # 키워드 추출
                articleContent.split()
                # mecab 이용 명사 추출
                nouns_mecab = mecab.nouns(articleContent)

                mecabWords=[]
                for noun in nouns_mecab:    # 1글짜 제거
                    if len(noun) != 1:
                        mecabWords.append(noun)
                words = Counter(mecabWords) # 키워드 빈도 채크
                print(words)
                # DB 저장(키워드)
                for word in words:
                    curs.execute("INSERT INTO newskeyword(url,word,quantity) VALUES (%s,%s,%s)",(url, word, words.get(word)))
            else:   # news info 중복 저장 방어
                print("-----중복 알림-----\n중복 뉴스: "+articleUrl)
                curs.execute("SELECT rankdate from ranknews where url='{url}'".format(url=url))
                result = curs.fetchall()
                for day in result:
                    print(day[0].strftime("%Y년%m월%d일>>")+rankUrl + day[0].strftime("%Y%m%d"))
                print('---------------')
    print('소요시간: '+str(datetime.datetime.now()-startTime)+'\n\n')
curs.close()
conn.close()
