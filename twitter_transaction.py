from selenium import webdriver
from profile_info import main_info, format_timestamp
from twitter_bd_insert import insert_main_info
from insert_functions import insert_row_twitchack
from insert_functions import select_twitchack
import hashlib
import datetime
from threading import Thread
from queue import Queue
import sys


def generate_used_users(user_info):
    # Идем по твитам и ответам и добавляем аккаунты, которые взаимодейтсвуют с запрашиваемым
    list_used_users = []
    for post in user_info['posts']:
        if post['user_id'] not in list_used_users:
            list_used_users.append(post['user_id'])
        for answer in post['answers_list']:
            if answer['user_id'] not in list_used_users:
                list_used_users.append(answer['user_id'])

    # Добавляем сюда же подписчиков и подписки
    for user in user_info['followers_list']:
        if user not in list_used_users:
            list_used_users.append(user)

    for user in user_info['following_list']:
        if user not in list_used_users:
            list_used_users.append(user)

    for user in list_used_users:
        try:
            select_user_on_userid(user)
            list_used_users.remove(user)
        except IndexError:
            pass

    return list_used_users


def additional_scrapping_used_users(users_list):
    browser = webdriver.Chrome(executable_path=r"chromedriver.exe")
    info_about_used_users = []
    for user in users_list:
        info_about_used_users.append(main_info(user, browser))

    return info_about_used_users


def select_user_on_userid(user_id):
    select_query = 'SELECT id from users WHERE username=%s'
    user_twithack_id = select_twitchack(select_query, [user_id, ])[0][0]
    return user_twithack_id


def create_multithreading(function, data):
    result_list = []

    # Формируем очередь потоков и исполянем функцию с поделенными данными по полам
    queue = Queue()
    threads_list = []
    thread1 = Thread(target=lambda q, arg1: q.put(function(arg1)), args=(queue, data[:len(data)//2]))
    thread1.start()
    threads_list.append(thread1)
    thread2 = Thread(target=lambda q, arg1: q.put(function(arg1)), args=(queue, data[len(data)//2:]))
    thread2.start()
    threads_list.append(thread2)

    # Подключаем потоки
    for thread in threads_list:
        thread.join()

    # Вовзразаем значения
    while not queue.empty():
        result_list += queue.get()

    return result_list


def bd_insert_used_users(used_users):
    # Вставляет инфу о всех нужных пользователях в свою бд
    for user in used_users:
        insert_main_info(user)


def twithack_bd_insert_interactions(destination, autor, target, type, date):
    query = 'INSERT INTO interactions(id, destination, autor, target, type, status, date, trash) ' \
            'VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ' \
            'ON DUPLICATE KEY UPDATE id=VALUES(id), destination=VALUES(destination), autor=VALUES(autor), ' \
            'target=VALUES(target), type=VALUES(type), status=VALUES(status), date=VALUES(date), trash=VALUES(trash)'

    if date:
        date = datetime.datetime.strftime(format_timestamp(date), "%Y-%m-%d %H:%M:%S")
    else:
        date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    info = [int(str(destination)+str(autor)+str(target)+str(type)) % sys.maxsize, destination, autor, target, type, "1", date, "0"]
    insert_row_twitchack(query, [info,])


# ALTER TABLE `users` AUTO_INCREMENT=3
def twithack_bd_insert_user_info(user_info):
    query = 'INSERT INTO users(username, name, location, country, password, email, date, website, bio, avatar, ' \
            'activation_code, type_account, mode, status, 	messages_private, email_notification_follow, ' \
            'email_notification_msg, language, oauth_uid, oauth_provider, twitter_oauth_token, ' \
            'twitter_oauth_token_secret) ' \
            'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ' \
            'ON DUPLICATE KEY UPDATE username=VALUES(username), name=VALUES(name), password=VALUES(password), ' \
            'email=VALUES(email), date=VALUES(date), avatar=VALUES(avatar), type_account=VALUES(type_account), ' \
            'mode=VALUES(mode), status=VALUES(status)'

    info_list = []
    for user in user_info:
        if not user["src_photo"]:
            avatar = "avatar.png"
        else:
            avatar = user["src_photo"]
        date = datetime.datetime.strftime(format_timestamp(user['date_of_registration']), "%Y-%m-%d %H:%M:%S")
        print(date)
        info_list.append([user['user_id'], user["name"], "", "xx", hashlib.sha1(b"123123").hexdigest(),
                          user['user_id']+"@yandex.ru", date, "", "", avatar,
                          "2455c61baa0699440e537dc155ee6250e10e7558VjvhPKj1at_dZ-VJlytJqQBuYDL5TFaDuGoZH6XF",
                          "0", "1", "active", "1", "1", "1", "en", "", "", "", ""])
    insert_row_twitchack(query, info_list)


def twithack_bd_insert_followers(user_info):
    # Проходим по подписчикам и подписываемся
    query = 'INSERT INTO followers(follower, following, status) ' \
                    'VALUES (%s, %s, %s) ' \
                    'ON DUPLICATE KEY UPDATE follower=VALUES(follower), following=VALUES(following), status=VALUES(status)'

    user_twithack_id = select_user_on_userid(user_info['user_id'])

    # Привязываем подписчиков и подписки
    followers_id = []
    follower_info = []
    for user in user_info['followers_list']:
        followers_id.append(select_user_on_userid(user))
    for follower_id in followers_id:
        info = [follower_id, user_twithack_id, "1"]
        twithack_bd_insert_interactions(user_twithack_id, follower_id, follower_id, "1", "")
        follower_info.append(info)

    for user in user_info['following_list']:
        followers_id.append(select_user_on_userid(user))
    for follower_id in followers_id:
        info = [user_twithack_id, follower_id, "1"]
        twithack_bd_insert_interactions(follower_id, user_twithack_id, user_twithack_id, "1", "")
        follower_info.append(info)

    insert_row_twitchack(query, follower_info)


def twithack_bd_insert_post(user_info, daterange):
    query = 'INSERT INTO posts(id, repost_of_id, user_id, token_id, post_details, photo, video_code, video_title, ' \
            'video_site,  video_url, user, status, video_thumbnail, date, status_general, url_soundcloud, ' \
            'title_soundcloud, thumbnail_song, doc_site, doc_url, url, url_thumbnail, url_title, url_description, ' \
            'url_host, geolocation) ' \
            'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)' \
            'ON DUPLICATE KEY UPDATE id=VALUES(id), repost_of_id=VALUES(repost_of_id), user_id=VALUES(user_id),' \
            'token_id=VALUES(token_id), post_details=VALUES(post_details), photo=VALUES(photo), user=VALUES(user) , ' \
            'status=VALUES(status), date=VALUES(date)'

    # Добавить фотки
    post_info = []
    for post in user_info['posts']:
        date = datetime.datetime.utcfromtimestamp(post['timestamp_of_tweet'])
        if date >= datetime.datetime.strptime(daterange[0], "%m.%d.%Y") and date <= datetime.datetime.strptime(daterange[1], "%m.%d.%Y"):
            info = [int(post['tweet_id']) % sys.maxsize, 0, 0,  hashlib.sha1(post['tweet_id'].encode()).hexdigest(),
                    post['text'], post['images_url'], "", "", "", "", select_user_on_userid(post['user_id']), "1", "",
                    date.strftime("%Y-%m-%d %H:%M:%S"), "1", "", "", "", "", "", "", "", "", "", "", ""]
            post_info.append(info)
            for retweet in user_info['retweeted_tweets_id']:
                if post['tweet_id'] == retweet['retweet_id']:
                    info = [int(retweet['id']) % sys.maxsize, int(retweet['retweet_id']) % sys.maxsize, select_user_on_userid(post['user_id']),
                            hashlib.sha1(retweet['id'].encode()).hexdigest(),  post['text'], post['images_url'], "", "", "", "",
                            select_user_on_userid(user_info['user_id']), "1", "", date.strftime("%Y-%m-%d %H:%M:%S"), "1",
                            "", "", "", "", "", "", "", "", "", "", ""]
                    twithack_bd_insert_interactions(select_user_on_userid(post['user_id']),
                                                    select_user_on_userid(user_info['user_id']),
                                                    int(retweet['retweet_id']) % sys.maxsize, "1", "")
                    post_info.append(info)
    insert_row_twitchack(query, post_info)


def twithack_bd_insert_answers(user_info, daterange):
    query = 'INSERT INTO reply(id, post, user, reply, date, status) ' \
            'VALUES (%s, %s, %s, %s, %s, %s) ' \
            'ON DUPLICATE KEY UPDATE id=VALUES(id), post=VALUES(post), user=VALUES(user), reply=VALUES(reply), ' \
            'date=VALUES(date), status=VALUES(status)'

    answer_info = []
    for post in user_info['posts']:
        for answer in post['answers_list']:
            if answer['text']:
                date = datetime.datetime.utcfromtimestamp(answer['timestamp_of_tweet'])
                if date >= datetime.datetime.strptime(daterange[0], "%m.%d.%Y") and date <= datetime.datetime.strptime(daterange[1], "%m.%d.%Y"):
                    info = [int(answer['tweet_id']) % sys.maxsize, int(post['tweet_id']) % sys.maxsize,
                            select_user_on_userid(answer['user_id']), answer['text'], date.strftime("%Y-%m-%d %H:%M:%S"), "1"]
                    answer_info.append(info)

    insert_row_twitchack(query, answer_info)


def twitter_transaction(user_info, daterange):
    # Отбор взаимодейтсвующих аккаунтов с переносимым
    used_users = generate_used_users(user_info)
    try:
        used_users.remove(user_info['name'])
    except ValueError:
        pass

    # Дополнительно скрапим используемых пользователей
    info_about_used_users = create_multithreading(additional_scrapping_used_users, used_users)

    # Вставляем информацию о используемых пользователях в своб бд
    bd_insert_used_users(info_about_used_users)

    # Создаем польхователя и доп пользователей в сети
    twithack_bd_insert_user_info([user_info] + info_about_used_users)

    # Подписываем пользователей на пользователя и его на них
    twithack_bd_insert_followers(user_info)

    # Выкладываем посты, в том числе и ретвиты
    daterange = daterange.split(' - ')
    twithack_bd_insert_post(user_info, daterange)
    twithack_bd_insert_answers(user_info, daterange)
