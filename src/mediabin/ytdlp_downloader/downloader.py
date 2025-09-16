import sys
from yt_dlp import YoutubeDL
from tqdm import tqdm

def initiate_download(url: str):
    pbar = None

    def progress_hook(d):
        nonlocal pbar
        if d['status'] == 'downloading':
            if pbar is None:
                # Initialize tqdm bar
                total_bytes = d.get('total_bytes') or d.get('total_bytes_estimate')
                if total_bytes:
                    pbar = tqdm(total=total_bytes, unit='B', unit_scale=True, desc=d['filename'])
                else:
                    pbar = tqdm(unit='B', unit_scale=True, desc=d['filename'])
            
            # Update tqdm total if it changes (e.g., estimate becomes accurate total)
            current_total = d.get('total_bytes') or d.get('total_bytes_estimate')
            if pbar and current_total and pbar.total != current_total:
                pbar.total = current_total
                pbar.refresh()

            if pbar and d.get('downloaded_bytes') is not None:
                pbar.update(d['downloaded_bytes'] - pbar.n)
        elif d['status'] == 'finished':
            if pbar:
                pbar.close()
            print(f"\nDownload finished for {d['filename']}")
        elif d['status'] == 'error':
            if pbar:
                pbar.close()
            print(f"\nError during download: {d['filename']}", file=sys.stderr)

    ydl_opts = {
        'outtmpl': './out/%(id)s.%(ext)s',
        'progress_hooks': [progress_hook],
        'noplaylist': True, # Ensure only single video is downloaded
        'quiet': True, # Suppress default yt-dlp output
        'noprogress': True, # Suppress default yt-dlp progress bar
    }
    with YoutubeDL(ydl_opts) as ydl:
        ydl.download([url])


if __name__ == "__main__":
    if len(sys.argv) > 1:
        initiate_download(sys.argv[1])
    else:
        print("Usage: python downloader.py <URL>")