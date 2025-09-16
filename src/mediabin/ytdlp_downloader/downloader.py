import sys
from yt_dlp import YoutubeDL

def initiate_download(url: str):
    ydl_opts = {
        'outtmpl': './out/%(title)s.%(ext)s',
    }
    with YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])


if __name__ == "__main__":
    if len(sys.argv) > 1:
        initiate_download(sys.argv[1])
    else:
        print("Usage: python downloader.py <URL>")