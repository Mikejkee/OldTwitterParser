from insert_functions import insert_row


def insert_main_info(user_info):
    query = 'INSERT INTO user_info(user_id, name, description, date_of_registration, date_of_birth, photo_src) ' \
            'VALUES (%s, %s, %s, %s, %s, %s) ' \
            'ON DUPLICATE KEY UPDATE user_id=VALUES(user_id), name=VALUES(name), description=VALUES(description), ' \
            'date_of_registration=VALUES(date_of_registration), date_of_birth=VALUES(date_of_birth), photo_src=VALUES(photo_src)'

    info = [user_info['user_id'], user_info["name"], "", user_info["date_of_registration"], user_info["date_of_birth"], user_info["src_photo"]]
    insert_row(query, [info, ])


def insert_follow_info(follow_list, user_id, param):
    query = 'INSERT INTO followers(follower_id, user_id) ' \
            'VALUES (%s, %s) ' \
            'ON DUPLICATE KEY UPDATE follower_id=VALUES(follower_id), user_id=VALUES(user_id)'

    follower_info = []
    # Если 0 - то значит подписчики, 1 - читающие
    if param == 0:
        for follower in follow_list:
            info = [follower, user_id]
            follower_info.append(info)
    elif param == 1:
        for follower in follow_list:
            info = [user_id, follower]
            follower_info.append(info)

    insert_row(query, follower_info)


def insert_posts(posts):
    query = 'INSERT INTO posts(post_id, user_id, count_of_likes, count_of_retweet, hashtags, images_url, ' \
            'liked_users, retweeted_user, text, timestamp_of_tweet) ' \
            'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)' \
            'ON DUPLICATE KEY UPDATE post_id=VALUES(post_id), user_id=VALUES(user_id), ' \
            'count_of_likes=VALUES(count_of_likes), count_of_retweet=VALUES(count_of_retweet), ' \
            'hashtags=VALUES(hashtags), images_url=VALUES(images_url), liked_users=VALUES(liked_users), ' \
            'retweeted_user=VALUES(retweeted_user), text=VALUES(text), timestamp_of_tweet=VALUES(timestamp_of_tweet)'

    posts_info = []
    for post in posts:
        hashtags_str = ""
        image_str = ""
        for hashtag in post['hashtags']:
            hashtags_str += hashtag + " "
        for image in post['images_url']:
            image_str += image + " "
        info = [post["tweet_id"], post['user_id'], post["count_of_likes"],
                post["count_of_retweet"], hashtags_str, image_str,
                str(post["liked_users"]), str(post["retweeted_user"]), post["text"], int(post["timestamp_of_tweet"])]
        posts_info.append(info)

    insert_row(query, posts_info)


def insert_answers(posts):
    query = 'INSERT INTO answers(answer_id, post_id, user_id, count_of_likes, count_of_retweet, hashtags, ' \
            'images_url, text, timestamp_of_tweet) ' \
            'VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ' \
            'ON DUPLICATE KEY UPDATE answer_id=VALUES(answer_id), post_id=VALUES(post_id), user_id=VALUES(user_id), ' \
            'count_of_likes=VALUES(count_of_likes), count_of_retweet=VALUES(count_of_retweet), ' \
            'hashtags=VALUES(hashtags), images_url=VALUES(images_url), text=VALUES(text), timestamp_of_tweet=VALUES(timestamp_of_tweet)'

    answers_info = []
    for post in posts:
        for answer in post["answers_list"]:
            hashtags_str = ""
            image_str = ""
            for hashtag in answer['hashtags']:
                hashtags_str += hashtag + " "
            for image in answer['images_url']:
                image_str += image + " "
            info = [answer["tweet_id"], post["tweet_id"], answer["user_id"], answer["count_of_likes"],
                    answer["count_of_retweet"], hashtags_str, image_str,
                    answer["text"], int(answer["timestamp_of_tweet"])]
            answers_info.append(info)

    insert_row(query, answers_info)


def insert_user_into_my_bd(user_info):
    # Вставляем основную информацию
    insert_main_info(user_info)
    insert_follow_info(user_info['followers_list'], user_info['user_id'], 0)
    insert_follow_info(user_info['following_list'], user_info['user_id'], 1)

    # Вставляем информацию о постах
    insert_posts(user_info['posts'])

    # Вставляем информацию об ответах к постам
    insert_answers(user_info['posts'])
