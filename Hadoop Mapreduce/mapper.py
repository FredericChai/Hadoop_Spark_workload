#!/usr/bin/python3
import os
import sys


def mapper():
    """ This mapper select specifi column from record and return the category_video information.
    Input format: 'video_id', 'trending_date', 'title', 'channel_title', 'category_id',
       'publish_time', 'tags', 'views', 'likes', 'dislikes', 'comment_count',
       'thumbnail_link', 'comments_disabled', 'ratings_disabled',
       'video_error_or_removed', 'description',dtype='object'
    Output format: category_id \t vedio_id
sample:
42  syiEqRf0Xp8
43  tR5FZC2FP-U
43  uLVck0MVPlk
43  uMJiVqREr_M
43  uMJiVqREr_M
    """
    filepath = os.environ["map_input_file"] #get multiple input
    filename = os.path.split(filepath)[-1]
    # filename = 'cacleaned.csv'
    for line in sys.stdin:
        # Clean input and split it
        parts = line.strip().split(",")

        # Check that the line is of the correct format
        # If line is malformed, we ignore the line and continue to the next line
        # if len(parts) != 6:
        #   continue
        if not parts[2].isdigit() or len(parts[1])!=11:
            continue

        v_id = parts[1].strip()
        c_id = parts[2].strip()
        if filename == 'cacleaned.csv':
            country = 'ca'
        if filename == 'uscleaned.csv':
            country = 'us'

            # In hadoop streaming, the output is sys.stdout, thus the print command
            # Do not use or add return
        print("{}\t{}\t{}".format(c_id,v_id,country))

if __name__ == "__main__":
    mapper()
