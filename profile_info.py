from selenium import webdriver
from bs4 import BeautifulSoup
import re
import time
from datetime import datetime
from selenium.common import exceptions
from selenium.webdriver.chrome.options import Options
import wget
import os


import locale
locale.setlocale(locale.LC_ALL, 'ru')


def list_followers(user, browser, param):
    # Вытаскиваем ссылки читаемых и читателей
    if param == 0:
        browser.get("https://twitter.com/" + user + "/following")
    elif param == 1:
        browser.get("https://twitter.com/" + user + "/followers")
    # Пролистываем список до конца (загружается js)
    last_height = browser.execute_script("return document.body.scrollHeight")
    while True:
        browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)
        new_height = browser.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
    html = BeautifulSoup(browser.page_source, "html.parser")
    users_list = []
    for user in html.find_all('a', class_="fullname ProfileNameTruncated-link u-textInheritColor js-nav")[1:]:
        users_list.append(user.attrs["href"][1:])
    return users_list


def format_timestamp(timestamp):
    if "мая" in timestamp:
        timestamp = re.sub(" мая ", ".05.", timestamp[:-3])
        timestamp = datetime.strptime(timestamp, "%H:%M - %d.%m.%Y")
    elif "сент." in timestamp:
        timestamp = re.sub(" сент. ", ".09.", timestamp[:-3])
        timestamp = datetime.strptime(timestamp, "%H:%M - %d.%m.%Y")
    elif "февр." in timestamp:
        timestamp = re.sub(" февр. ", ".02.", timestamp[:-3])
        timestamp = datetime.strptime(timestamp, "%H:%M - %d.%m.%Y")
    elif "нояб." in timestamp:
        timestamp = re.sub(" нояб. ", ".11.", timestamp[:-3])
        timestamp = datetime.strptime(timestamp, "%H:%M - %d.%m.%Y")
    else:
        timestamp = datetime.strptime(timestamp[:-3], "%H:%M - %d %b. %Y")
    return timestamp


def info_about_tweet(html, browser):
    list_retweeted_users = []
    list_liked_users = []
    # Фоловеры и лайкнуйвшие (проблема, что твиттер дает увидеть только последние 25)
    # try:
    #     browser.find_element_by_class_name("request-retweeted-popup").click()
    #     for retweeted_user in html.find_all('div', class_="js-actionable-user js-profile-popup-actionable"):
    #         list_retweeted_users.append(retweeted_user.attrs["data-screen-name"])
    #     browser.find_elements_by_css_selector(".modal-btn.modal-close.js-close")[8].click()
    #     time.sleep(1)
    # except exceptions.NoSuchElementException:
    #     pass
    # try:
    #     browser.find_element_by_css_selector(".request-favorited-popup").click()
    #     html = BeautifulSoup(browser.page_source, "html.parser")
    #     for liked_user in html.find_all('div', class_="js-actionable-user js-profile-popup-actionable"):
    #         list_liked_users.append(liked_user.attrs["data-screen-name"])
    #     browser.find_elements_by_css_selector(".modal-btn.modal-close.js-close")[8].click()
    #     time.sleep(1)
    # except exceptions.NoSuchElementException:
    #     pass

    user_id = re.search("com/(.*?)/status", browser.current_url)[1]
    tweet_id = re.search("status/(.*)", browser.current_url)[1]

    try:
        text = html.find('p', class_="TweetTextSize TweetTextSize--jumbo js-tweet-text tweet-text").text
        hashtags = re.findall("#.*? ", text)
    except:
        text = ""
        hashtags = ""

    timestamp = format_timestamp(html.find('span', class_="metadata").contents[1].text).timestamp()

    try:
        count_retweets = html.find('a', class_="request-retweeted-popup").attrs['data-tweet-stat-count']
    except:
        count_retweets = 0
    try:
        cout_likes = html.find('a', class_="request-favorited-popup").attrs['data-tweet-stat-count']
    except:
        cout_likes = 0

    try:
        images_url = []
        image_tags = html.find('div', class_="permalink-inner permalink-tweet-container").contents[1].contents[5].contents[1].contents[1].contents[1]
        for number_image in range(1, len(image_tags), 2):
            if len(image_tags) == 3:
                images_url.append(image_tags.contents[number_image].attrs['data-image-url'])
                break
            images_url.append(image_tags.contents[number_image].contents[1].attrs['data-image-url'])
    except:
        images_url = ""

    # Выкачиваем фото

    try:
        src_photo = re.search(".*\/(.*)", images_url[0])
        path_src_photo = "../../dev/social-microblogging-pro-1.7.1-rus/Script/upload/" + src_photo[1]
        if not os.path.exists(path_src_photo):
            filename = wget.download(images_url[0])
            os.rename(filename, path_src_photo)
        src_photo = src_photo[1]
    except:
        src_photo = ""

    tweet_info = {
            'user_id': user_id,
            'tweet_id': tweet_id,
            'text': text,
            'hashtags': hashtags,
            'timestamp_of_tweet': timestamp,
            'count_of_retweet': count_retweets,
            'retweeted_user': list_retweeted_users,
            'count_of_likes': cout_likes,
            'liked_users': list_liked_users,
            'images_url': src_photo,
        }
    return tweet_info


def info_about_answer(answer_tag):

    html = BeautifulSoup(answer_tag.get_attribute('innerHTML'), "html.parser")

    tweet_id = answer_tag.get_attribute("data-item-id")
    user_id = html.contents[1].attrs["data-screen-name"]

    try:
        text = html.find('p', class_="TweetTextSize js-tweet-text tweet-text").text
        hashtags = re.findall("#.*? ", text)
    except:
        text = ""
        hashtags = ""

    timestamp = format_timestamp(html.find('small', class_="time").contents[1].attrs["title"]).timestamp()

    try:
        count_retweets = html.find('button', class_="ProfileTweet-actionButton js-actionButton js-actionRetweet").contents[3].contents[1].text
        if count_retweets == "":
            count_retweets = 0
    except:
        count_retweets = 0

    try:
        count_likes = html.find('button', class_="ProfileTweet-actionButton js-actionButton js-actionFavorite").contents[3].contents[1].text
        if count_likes == "":
            count_likes = 0
    except:
        count_likes = 0

    try:
        images_url = []
        for url in html.findAll('div', class_="AdaptiveMedia-photoContainer js-adaptive-photo "):
            images_url.append(url.contents[1].attrs["src"])
    except:
        images_url = ""

    tweet_info = {
            'user_id': user_id,
            'tweet_id': tweet_id,
            'text': text,
            'hashtags': hashtags,
            'timestamp_of_tweet': timestamp,
            'count_of_retweet': count_retweets,
            'count_of_likes': count_likes,
            'images_url': images_url,
        }
    return tweet_info


def get_tweets(user, browser, answers_flag, daterange):
    # Пролистываем профиль до конца
    # Это ссылка с ответами (пока не продумано, что делать с ответами пользователя на другие твиты)
    # browser.get("https://twitter.com/" + user + "/with_replies")
    browser.get("https://twitter.com/" + user)
    last_height = browser.execute_script("return document.body.scrollHeight")
    key = 0;
    while key != 10:
        browser.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)
        new_height = browser.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height
        key += 1

    html = BeautifulSoup(browser.page_source, "html.parser")
    list_id_of_tweets = []
    list_id_of_retweets = []
    # Собираем все айдишники твитов
    for tweet in html.findAll("div", class_="stream-item-header"):
        try:
            id = re.search("status/(.*)", tweet.contents[3].contents[1].attrs["href"])[1]
            date = format_timestamp(tweet.contents[3].contents[1].attrs["title"]).timestamp()
            if datetime.utcfromtimestamp(date) >= datetime.strptime(daterange[0], "%m.%d.%Y") and datetime.utcfromtimestamp(date) <= datetime.strptime(daterange[1], "%m.%d.%Y"):
                list_id_of_tweets.append(id)
            if "ретвитнул" in tweet.parent.parent.text:
                list_id_of_retweets.append({
                    'id': tweet.parent.parent.attrs['data-retweet-id'],
                    'retweet_id': id,
                    }
                )
        except IndexError:
            continue

    list_tweets = []
    # Проходим по каждому айдишнику и собираем иформацию о твите и ответах к нему
    for tweet_id in list_id_of_tweets:
        browser.get("https://twitter.com/" + user + "/status/" + tweet_id)
        time.sleep(1)

        # Инфо о твите
        html = BeautifulSoup(browser.page_source, "html.parser")
        tweet_info = info_about_tweet(html, browser)
        browser.refresh()

        if answers_flag == 1:
            # Прокрутка ответов до конца
            dialog = browser.find_element_by_css_selector(".PermalinkOverlay.PermalinkOverlay-with-background.load-at-boot")
            # Прокручивем его до конца (js)
            last_height = browser.execute_script("return arguments[0].scrollTop", dialog)
            while True:
                browser.execute_script('arguments[0].scrollTop = arguments[0].scrollTop + arguments[0].offsetHeight;',
                                       dialog)
                time.sleep(2)
                new_height = browser.execute_script("return arguments[0].scrollTop", dialog)
                if new_height == last_height:
                    break
                last_height = new_height

            # Скрапим ответы к твитам
            answers_list = []
            for answer_tag in browser.find_elements_by_css_selector(".js-stream-item.stream-item.stream-item"):
                if answer_tag.text != "Этот твит недоступен.":
                    answers_list.append(info_about_answer(answer_tag))

        # Добавляем к информации о твите информацию об ответах
        tweet_info["answers_list"] = answers_list

        # Закрываем твит
        browser.find_element_by_css_selector(".PermalinkProfile-dismiss.modal-close-fixed").click()
        time.sleep(1)

        # Добавлям информацию к общей информации
        list_tweets.append(tweet_info)

    return list_tweets, list_id_of_retweets


def main_info(user, browser):
    # Переходим на нужного пользователя
    browser.get("https://twitter.com/" + user)
    html_profile = BeautifulSoup(browser.page_source, "html.parser")
    # Количество подписчиков и подписок
    nav = html_profile.find_all('span', class_="ProfileNav-value")
    # Имя профиля, дата регистрации, дата рождения
    name = html_profile.find('h1', class_="ProfileHeaderCard-name").contents[1].text
    date_of_registration = html_profile.find('div', class_="ProfileHeaderCard-joinDate").contents[3].attrs["title"]
    try:
        date_of_birth = re.sub(r".*:(.*)\..*", "\\1",
                               html_profile.find('span', class_="ProfileHeaderCard-birthdateText u-dir").contents[
                                   0].text)[:-2]
    except AttributeError:
        date_of_birth = ""
    # Выкачиваем фото профиля
    src_photo = html_profile.find('img', class_="ProfileAvatar-image").attrs["src"]
    format = re.search(".*(\..*)", src_photo)
    path_src_photo = "../../dev/social-microblogging-pro-1.7.1-rus/Script/public/avatar/" + user + format[1]
    src_photo = user + format[1]
    if not os.path.exists(path_src_photo):
        filename = wget.download(html_profile.find('img', class_="ProfileAvatar-image").attrs["src"])
        os.rename(filename,  path_src_photo)
    try:
        count_followers = nav[2].text
    except:
        count_followers = 0

    try:
        count_following = nav[1].text
    except:
        count_following = 0

    main_info = {
        'user_id': user,
        "name": name,
        "date_of_registration": date_of_registration,
        "date_of_birth": date_of_birth,
        "src_photo": src_photo,
        'count_followers': count_followers,
        "count_following": count_following,
    }

    return main_info

def space(text):
    for x in text:
        if x.isspace():
            return True
    return False


def twitter_profile_info(user, tweets_flag, followers_flag, answers_flag, daterange):
    login = "MKurpatov"
    password = "119988mi"
    executable_path = r"chromedriver.exe"
    chrome_options = Options()
    chrome_options.add_extension('old_twitter.crx')

    browser = webdriver.Chrome(executable_path=executable_path, chrome_options=chrome_options)
    # Авторизовываемся в твиттере для сбора информации нужного профиля (иначе некоторая информация недоступна)
    browser.get("https://twitter.com/login")
    browser.find_element_by_class_name("js-username-field").send_keys(login)
    password_input = browser.find_element_by_class_name("js-password-field")
    password_input.send_keys(password)
    password_input.submit()

    page_info = main_info(user, browser)
    if space(str(page_info['count_followers'])) or space(str(page_info['count_following'])):
        return 0
    # Выкачиваем посты, если требуется
    posts = []
    retweeted_tweets_id = []
    daterange = daterange.split(' - ')
    if tweets_flag == 1:
        posts, retweeted_tweets_id = get_tweets(user, browser, answers_flag, daterange)

    # Выкачиваем читателей и читаемых, если требуется
    following_list = []
    followers_list = []
    if followers_flag == 1:
        following_list = list_followers(user, browser, 0)
        followers_list = list_followers(user, browser, 1)

    twitter_user_info = {
        'user_id': user,
        'name': page_info["name"],
        'date_of_registration': page_info["date_of_registration"],
        'date_of_birth': page_info["date_of_birth"],
        'src_photo': page_info["src_photo"],
        'following_list': following_list,
        'followers_list': followers_list,
        "retweeted_tweets_id": retweeted_tweets_id,
        'posts': posts,
    }
    browser.close()
    return twitter_user_info

# from pprint import pprint
# pprint(twitter_profile_info('dim0nius', 1, 1, 1))

# Собрать картинку с профиоля
# Не вытаскивает видосы
# Собирать верификацию
# Посмотреть список лайкнувших и ретвитнувших
# Не вписывать на что ответил пользователь, либо продумать, что если он ответил сохранять пост на который ответил и его ответ
# Не сохраняет фотку, если это ответ на твит
# Не сохраняет смайлы
# Если чистый ответ, то делает лишние переходы туда сюда
# Нажимать на кнопку "показать больше ответов"
# Если это ответ на какой-то левый твит не собирает информацию об этотм твите( именно когда овтет на ответ)
# Парсить описание профиля


