import psycopg2 as pg2

# DB connection
conn = pg2.connect(dbname='postgres', user='postgres', password='12300', host='127.0.0.1', port='5432')
curs = conn.cursor()

f = open('exceptionText.txt', mode='rt', encoding='UTF8')
for word in f.readlines():
    word_s = word.strip('\n')
    print("예외단어: " + word_s + "  를(을) 제거합니다.  .  .  .", end='')
    curs.execute("DELETE FROM newskeyword WHERE word='{word}'".format(word=word_s))
    conn.commit()
    print("제거 완료!")
print("작업 끝!")
