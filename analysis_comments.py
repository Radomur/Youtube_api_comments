from googleapiclient.discovery import build
import pandas as pd
import re

api_key = "*********" # api https://console.cloud.google.com/apis/
video_id = "VpA4KfX68rI" # с url видео

youtube = build("youtube", "v3", developerKey=api_key)

comments = []
nextToken = None

while True:
    resp = youtube.commentThreads().list(
        part="snippet,replies",
        videoId=video_id,
        textFormat="plainText",
        maxResults=100,
        pageToken=nextToken
    ).execute()

    for item in resp["items"]:
        top = item["snippet"]["topLevelComment"]["snippet"]
        comments.append({
            "author": top["authorDisplayName"],
            "text": top["textDisplay"],
            "likes": top["likeCount"],
            "published": top["publishedAt"]
        })
        # ответы
        if item.get("replies"):
            for r in item["replies"]["comments"]:
                rep = r["snippet"]
                comments.append({
                    "author": rep["authorDisplayName"],
                    "text": rep["textDisplay"],
                    "likes": rep["likeCount"],
                    "published": rep["publishedAt"]
                })

    nextToken = resp.get("nextPageToken")
    if not nextToken:
        break

df = pd.DataFrame(comments)
df.to_csv("comments.csv", index=False)
print("Готово! Экспорт в comments.csv")

# Анализ комментариев с кошельками
# Паттерн для поиска адресов кошельков формата gonka1...
# Общая длина адреса 40-45 символов, префикс gonka1 = 6 символов
# Значит после префикса: 34-39 символов
wallet_pattern = r'\bgonka1[a-z0-9]{34,39}\b'

# Находим все комментарии с кошельками
comments_with_wallets = []
wallet_stats = {}  # Для подсчета статистики по кошелькам

for comment in comments:
    wallets = re.findall(wallet_pattern, comment["text"], re.IGNORECASE)
    if wallets:
        for wallet in wallets:
            wallet_lower = wallet.lower()
            if wallet_lower not in wallet_stats:
                wallet_stats[wallet_lower] = []
            wallet_stats[wallet_lower].append({
                "author": comment["author"],
                "text": comment["text"],
                "likes": comment["likes"],
                "published": comment["published"]
            })

# Создаем список комментариев с уникальными кошельками (только первый по дате)
unique_wallet_comments = []
for wallet, comment_list in wallet_stats.items():
    # Сортируем по дате публикации
    sorted_comments = sorted(comment_list, key=lambda x: x["published"])
    # Берем только первый (самый ранний) комментарий
    first_comment = sorted_comments[0]
    first_comment["wallet"] = wallet
    unique_wallet_comments.append(first_comment)

# Создаем DataFrame с уникальными кошельками
if unique_wallet_comments:
    df_wallets = pd.DataFrame(unique_wallet_comments)
    # Конвертируем published в datetime для правильной сортировки
    df_wallets["published"] = pd.to_datetime(df_wallets["published"])
    # Сортируем по дате публикации
    df_wallets = df_wallets.sort_values("published")
    # Сохраняем в отдельный файл
    df_wallets.to_csv("comments_with_wallets.csv", index=False)

    print("\n" + "=" * 60)
    print("ОТЧЕТ ПО КОШЕЛЬКАМ")
    print("=" * 60)
    print(f"\nВсего найдено уникальных кошельков: {len(wallet_stats)}")
    print(f"Комментариев с кошельками сохранено: {len(unique_wallet_comments)}")

    # Выводим статистику по дубликатам
    print("\nСтатистика по кошелькам:")
    print("-" * 60)

    # Сортируем кошельки по количеству упоминаний (по убыванию)
    sorted_wallets = sorted(wallet_stats.items(), key=lambda x: len(x[1]), reverse=True)

    for wallet, comment_list in sorted_wallets:
        count = len(comment_list)
        if count > 1:
            print(f"Кошелек {wallet} встречается {count} раз в комментариях")
            print(f"  └─ Оставлен комментарий от {comment_list[0]['published']} (самый ранний)")
            print(f"  └─ Проигнорировано дубликатов: {count - 1}")
        else:
            print(f"Кошелек {wallet} встречается 1 раз в комментариях")

    print("\n" + "=" * 60)
    print(f"Файл с уникальными кошельками сохранен: comments_with_wallets.csv")
    print("=" * 60)
else:
    print("\nКомментариев с кошельками не найдено.")