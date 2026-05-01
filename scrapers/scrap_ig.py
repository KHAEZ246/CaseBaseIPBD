import instaloader
import pandas as pd
import os
from datetime import datetime

def scrape_by_hashtag(hashtag):
    L = instaloader.Instaloader()
    output_dir = '/opt/airflow/data/raw'
    os.makedirs(output_dir, exist_ok=True)
    
    posts_data = []
    print(f"Mencari postingan dengan hashtag: #{hashtag}")
    
    try:
        # Mengambil postingan berdasarkan hashtag
        hashtag_obj = instaloader.Hashtag.from_name(L.context, hashtag)
        
        # Ambil 15 postingan terbaru (Top Posts biasanya lebih relevan)
        for post in hashtag_obj.get_posts():
            if len(posts_data) >= 15:
                break
            
            # Filter sederhana: Cari kata kunci di caption
            keywords = ['konser', 'bola', 'stadion', 'manahan', 'sukoharjo', 'macet']
            caption = post.caption.lower() if post.caption else ""
            
            if any(key in caption for key in keywords):
                posts_data.append({
                    'hashtag': hashtag,
                    'post_id': post.shortcode,
                    'timestamp': post.date_local,
                    'likes': post.likes,
                    'caption': caption[:200], # Potong caption agar CSV tidak berantakan
                    'url': f"https://www.instagram.com/p/{post.shortcode}/"
                })

        if posts_data:
            df = pd.DataFrame(posts_data)
            filename = f"hashtag_{hashtag}_{datetime.now().strftime('%Y%m%d')}.csv"
            df.to_csv(f"{output_dir}/{filename}", index=False)
            print(f"Berhasil menyimpan {len(posts_data)} data hashtag #{hashtag}")
        else:
            print(f"Tidak ditemukan postingan yang relevan untuk #{hashtag}")

    except Exception as e:
        print(f"Error pada hashtag #{hashtag}: {e}")

if __name__ == "__main__":
    # Kamu bisa menjalankan beberapa hashtag sekaligus
    tags = ['persisday', 'stadionmanahan', 'eventsolo']
    for t in tags:
        scrape_by_hashtag(t)