import time, glob
import shutil

def without_shutil_text(outfilename):
    with open(outfilename, 'wb') as outfile:
        for filename in glob.glob('data/pm25/*.nc'):
            if filename == outfilename:
                # don't want to copy the output into the output
                continue
            with open(filename, 'rb') as readfile:
                outfile.write(readfile.read())

without_shutil_text("pm25_history.nc")