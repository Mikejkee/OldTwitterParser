from profile_info import twitter_profile_info
from twitter_bd_insert import insert_user_into_my_bd
from twitter_transaction import twitter_transaction
import json
from pprint import pprint

def twitter_scrapping_and_transaction(user_id, tweets_flag, followers_flag, answers_flag, daterange):
    # Скрапим информацию о пользователе
    print("Scrapping...")
    try:
        user_info = json.load(open('./users/'+user_id+".txt"))
    except:
        user_info = twitter_profile_info(user_id, tweets_flag, followers_flag, answers_flag, daterange)
        if user_info == 0:
            print("DONE!")
            return 0
        json.dump(user_info, open('users/'+user_id+".txt", 'w'))
        print("DONE!")

        # Вставляем информацию в базу данных
        print("Insert into my bd...")
        insert_user_into_my_bd(user_info)
        print("DONE!")

        # Переносим на ресурс
        print("Transaction...")
        twitter_transaction(user_info, daterange)
        print("DONE!")

        pprint(user_info)
    return user_info


list_scraped = []
first = twitter_scrapping_and_transaction('fukinncat', 1, 1, 1, '10.14.2019 - 11.14.2019')
list_scraped.append('fukinncat')
for follower in first['followers_list']+first['following_list']:
    try:
        if follower not in list_scraped:
            follower_all = twitter_scrapping_and_transaction(follower, 1, 1, 1, '10.14.2019 - 11.14.2019')
            list_scraped.append(follower)
            if follower_all != 0:
                for user in follower_all['followers_list'] + follower_all['following_list']:
                    if user not in list_scraped:
                        twitter_scrapping_and_transaction(user, 1, 1, 1, '10.14.2019 - 11.14.2019')
                        list_scraped.append(user)
    except:
        continue
