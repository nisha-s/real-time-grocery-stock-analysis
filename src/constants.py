import os

# Project root (one level up from this src/ directory). All paths below are
# resolved against it so the pipeline runs correctly regardless of the current
# working directory.
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

STREAM_INTERVAL = 3 # seconds between frames per feed
MAX_FRAMES = 5 # how many frames to send per feed
RANDOM_DELAY = True # add small random jitter
SHUFFLE_FRAMES = True # randomize order of images
USE_MULTIPROCESS = True # run feeds in parallel True=parallel, False=one by one

OUTPUT_DIR = os.path.join(BASE_DIR, "stream_input")
LOG_FILE = os.path.join(BASE_DIR, "feed_log.csv")

# camera feed paths considered (GroZi-120 inSitu video folders)
FEEDS = {
    "feed_1": os.path.join(BASE_DIR, "inSitu", "1", "video"),
    "feed_2": os.path.join(BASE_DIR, "inSitu", "14", "video"),
    "feed_3": os.path.join(BASE_DIR, "inSitu", "25", "video"),
    "feed_4": os.path.join(BASE_DIR, "inSitu", "90", "video"),
    "feed_5": os.path.join(BASE_DIR, "inSitu", "120", "video"),
}
